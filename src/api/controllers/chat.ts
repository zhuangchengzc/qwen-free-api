import { URL } from "url";
import { PassThrough } from "stream";
import http2 from "http2";
import path from "path";
import _ from "lodash";
import mime from "mime";
import FormData from "form-data";
import axios, { AxiosResponse } from "axios";

import APIException from "@/lib/exceptions/APIException.ts";
import EX from "@/api/consts/exceptions.ts";
import { createParser } from "eventsource-parser";
import logger from "@/lib/logger.ts";
import util from "@/lib/util.ts";

// 模型名称
const MODEL_NAME = "qwen";
// 最大重试次数
const MAX_RETRY_COUNT = 3;
// 重试延迟
const RETRY_DELAY = 5000;
// 伪装headers
const FAKE_HEADERS = {
  Accept: "application/json, text/plain, */*",
  "Accept-Encoding": "gzip, deflate, br, zstd",
  "Accept-Language": "zh-CN,zh;q=0.9",
  "Cache-Control": "no-cache",
  Origin: "https://tongyi.aliyun.com",
  Pragma: "no-cache",
  "Sec-Ch-Ua":
    '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
  "Sec-Ch-Ua-Mobile": "?0",
  "Sec-Ch-Ua-Platform": '"Windows"',
  "Sec-Fetch-Dest": "empty",
  "Sec-Fetch-Mode": "cors",
  "Sec-Fetch-Site": "same-site",
  Referer: "https://tongyi.aliyun.com/",
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  "X-Platform": "pc_tongyi",
  "X-Xsrf-Token": "48b9ee49-a184-45e2-9f67-fa87213edcdc",
};
// 文件最大大小
const FILE_MAX_SIZE = 100 * 1024 * 1024;

/**
 * 移除会话
 *
 * 在对话流传输完毕后移除会话，避免创建的会话出现在用户的对话列表中
 *
 * @param convId 会话ID (sessionId)
 * @param ticket tongyi_sso_ticket或login_aliyunid_ticket
 */
async function removeConversation(convId: string, ticket: string) {
  logger.info(`[removeConversation] 开始删除会话: ${convId}`);
  const result = await axios.post(
    `https://qianwen.biz.aliyun.com/dialog/session/delete`,
    {
      sessionId: convId,
    },
    {
      headers: {
        Cookie: generateCookie(ticket),
        ...FAKE_HEADERS,
      },
      timeout: 15000,
      validateStatus: () => true,
    }
  );
  checkResult(result);
  logger.success(`[removeConversation] 会话删除成功: ${convId}`);
}

/**
 * 同步对话补全
 *
 * @param model 模型名称
 * @param messages 参考gpt系列消息格式，多轮对话请完整提供上下文
 * @param ticket tongyi_sso_ticket或login_aliyunid_ticket
 * @param refConvId 引用的会话ID
 * @param retryCount 重试次数
 * @param tools 工具列表
 * @param toolChoice 工具选择策略
 */
async function createCompletion(
  model = MODEL_NAME,
  messages: any[],
  ticket: string,
  refConvId = '',
  retryCount = 0,
  tools?: any[],
  toolChoice?: any
) {
  let session: http2.ClientHttp2Session;
  return (async () => {
    logger.info(messages);

    // 提取引用文件URL并上传qwen获得引用的文件ID列表
    const refFileUrls = extractRefFileUrls(messages);
    const refs = refFileUrls.length
      ? await Promise.all(
          refFileUrls.map((fileUrl) => uploadFile(fileUrl, ticket))
        )
      : [];

    // 如果引用对话ID不正确则重置引用
    if (!/[0-9a-z]{32}/.test(refConvId))
      refConvId = '';

    // 处理工具调用
    const hasTools = tools && tools.length > 0;

    // 请求流
    const session: http2.ClientHttp2Session = await new Promise(
      (resolve, reject) => {
        const session = http2.connect("https://qianwen.biz.aliyun.com");
        session.on("connect", () => resolve(session));
        session.on("error", reject);
      }
    );
    const [refSessionId, parentMsgId = ''] = refConvId.split('-');
    const req = session.request({
      ":method": "POST",
      ":path": "/dialog/conversation",
      "Content-Type": "application/json",
      Cookie: generateCookie(ticket),
      ...FAKE_HEADERS,
      Accept: "text/event-stream",
    });
    req.setTimeout(120000);
    req.write(
      JSON.stringify({
        mode: "chat",
        model: "",
        action: "next",
        userAction: "chat",
        requestId: util.uuid(false),
        sessionId: refSessionId,
        sessionType: "text_chat",
        parentMsgId,
        params: {
          "fileUploadBatchId": util.uuid()
        },
        contents: messagesPrepare(messages, refs, !!refConvId, tools),
      })
    );
    req.setEncoding("utf8");
    const streamStartTime = util.timestamp();
    // 接收流为输出文本
    const answer = await receiveStream(req, hasTools);
    session.close();
    logger.success(
      `Stream has completed transfer ${util.timestamp() - streamStartTime}ms`
    );

    // 如果是临时创建的会话（非引用会话），则删除
    // 异步移除会话，如果消息不合规，此操作可能会抛出数据库错误异常，请忽略
    if (!refSessionId) {
      // answer.id 格式为 "sessionId-messageId"，需要提取 sessionId
      const sessionId = answer.id.split('-')[0];
      logger.info(`[会话清理] 准备删除临时会话: ${sessionId}`);
      removeConversation(sessionId, ticket).catch((err) => {
        logger.error(`[会话清理] 删除会话失败: ${sessionId}`, err);
      });
    } else {
      logger.info(`[会话清理] 保留引用会话: ${refSessionId}`);
    }

    return answer;
  })().catch((err) => {
    session && session.close();
    if (retryCount < MAX_RETRY_COUNT) {
      logger.error(`Stream response error: ${err.message}`);
      logger.warn(`Try again after ${RETRY_DELAY / 1000}s...`);
      return (async () => {
        await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY));
        return createCompletion(model, messages, ticket, refConvId, retryCount + 1, tools, toolChoice);
      })();
    }
    throw err;
  });
}

/**
 * 流式对话补全
 *
 * @param model 模型名称
 * @param messages 参考gpt系列消息格式，多轮对话请完整提供上下文
 * @param ticket tongyi_sso_ticket或login_aliyunid_ticket
 * @param refConvId 引用的会话ID
 * @param retryCount 重试次数
 * @param tools 工具列表
 * @param toolChoice 工具选择策略
 */
async function createCompletionStream(
  model = MODEL_NAME,
  messages: any[],
  ticket: string,
  refConvId = '',
  retryCount = 0,
  tools?: any[],
  toolChoice?: any
) {
  let session: http2.ClientHttp2Session;
  return (async () => {
    logger.info(messages);

    // 处理工具调用：如果有工具定义，先用非流式获取完整响应，再模拟流式输出
    const hasTools = tools && tools.length > 0;
    if (hasTools) {
      logger.info('[流式工具调用] 检测到工具定义，使用非流式模式获取响应后模拟流式输出');
      
      // 调用非流式接口获取完整响应
      const completion = await createCompletion(model, messages, ticket, refConvId, retryCount, tools, toolChoice);
      
      const choice = completion.choices[0];
      
      // 检查是否真的有工具调用
      const hasActualToolCalls = choice.message.tool_calls && choice.message.tool_calls.length > 0;
      
      if (!hasActualToolCalls && !choice.message.content) {
        // 模型返回空响应，可能是拒绝回答或其他原因，走正常流式
        logger.warn('[流式工具调用] 模型返回空响应，切换到正常流式处理');
        // 继续执行下面的正常流式逻辑/*  */
      } else {
        // 创建模拟的流式响应
        const transStream = new PassThrough();
        const created = util.unixTimestamp();
        
        // 发送初始消息
        transStream.write(`data: ${JSON.stringify({
          id: completion.id,
          model: completion.model,
          object: "chat.completion.chunk",
          choices: [{
            index: 0,
            delta: { role: "assistant", content: "" },
            finish_reason: null
          }],
          created
        })}\n\n`);
        
        const choice = completion.choices[0];
        
        // 如果有工具调用，发送工具调用信息
        if (choice.message.tool_calls && choice.message.tool_calls.length > 0) {
          for (const toolCall of choice.message.tool_calls) {
            transStream.write(`data: ${JSON.stringify({
              id: completion.id,
              model: completion.model,
              object: "chat.completion.chunk",
              choices: [{
                index: 0,
                delta: {
                  tool_calls: [{
                    index: 0,
                    id: toolCall.id,
                    type: toolCall.type,
                    function: {
                      name: toolCall.function.name,
                      arguments: toolCall.function.arguments
                    }
                  }]
                },
                finish_reason: null
              }],
              created
            })}\n\n`);
          }
        }
        
        // 如果有内容，分块发送（模拟打字效果）
        if (choice.message.content) {
          const content = choice.message.content;
          const chunkSize = 5; // 每次发送5个字符
          for (let i = 0; i < content.length; i += chunkSize) {
            const chunk = content.substring(i, i + chunkSize);
            transStream.write(`data: ${JSON.stringify({
              id: completion.id,
              model: completion.model,
              object: "chat.completion.chunk",
              choices: [{
                index: 0,
                delta: { content: chunk },
                finish_reason: null
              }],
              created
            })}\n\n`);
          }
        }
        
        // 发送结束标记
        transStream.write(`data: ${JSON.stringify({
          id: completion.id,
          model: completion.model,
          object: "chat.completion.chunk",
          choices: [{
            index: 0,
            delta: {},
            finish_reason: choice.finish_reason
          }],
          created
        })}\n\n`);
        
        transStream.end("data: [DONE]\n\n");
        
        logger.success('[流式工具调用] 模拟流式输出完成');
        return transStream;
      }
    }

    // 原有的流式处理逻辑（无工具调用时）
    // 提取引用文件URL并上传qwen获得引用的文件ID列表
    const refFileUrls = extractRefFileUrls(messages);
    const refs = refFileUrls.length
      ? await Promise.all(
          refFileUrls.map((fileUrl) => uploadFile(fileUrl, ticket))
        )
      : [];

    // 如果引用对话ID不正确则重置引用
    if (!/[0-9a-z]{32}/.test(refConvId))
      refConvId = ''

    // 请求流
    session = await new Promise((resolve, reject) => {
      const session = http2.connect("https://qianwen.biz.aliyun.com");
      session.on("connect", () => resolve(session));
      session.on("error", reject);
    });
    const [refSessionId, parentMsgId = ''] = refConvId.split('-');
    const req = session.request({
      ":method": "POST",
      ":path": "/dialog/conversation",
      "Content-Type": "application/json",
      Cookie: generateCookie(ticket),
      ...FAKE_HEADERS,
      Accept: "text/event-stream",
    });
    req.setTimeout(120000);
    req.write(
      JSON.stringify({
        mode: "chat",
        model: "",
        action: "next",
        userAction: "chat",
        requestId: util.uuid(false),
        sessionId: refSessionId,
        sessionType: "text_chat",
        parentMsgId,
        params: {
          "fileUploadBatchId": util.uuid()
        },
        contents: messagesPrepare(messages, refs, !!refConvId, tools),
      })
    );
    req.setEncoding("utf8");
    const streamStartTime = util.timestamp();
    // 创建转换流将消息格式转换为gpt兼容格式
    return createTransStream(req, hasTools, (convId: string) => {
      // 关闭请求会话
      session.close();
      logger.success(
        `Stream has completed transfer ${util.timestamp() - streamStartTime}ms`
      );
      // 如果是临时创建的会话（非引用会话），则删除
      // 流传输结束后异步移除会话，如果消息不合规，此操作可能会抛出数据库错误异常，请忽略
      if (!refSessionId) {
        logger.info(`[会话清理] 准备删除临时会话: ${convId}`);
        removeConversation(convId, ticket).catch((err) => {
          logger.error(`[会话清理] 删除会话失败: ${convId}`, err);
        });
      } else {
        logger.info(`[会话清理] 保留引用会话: ${refSessionId}`);
      }
    });
  })().catch((err) => {
    session && session.close();
    if (retryCount < MAX_RETRY_COUNT) {
      logger.error(`Stream response error: ${err.message}`);
      logger.warn(`Try again after ${RETRY_DELAY / 1000}s...`);
      return (async () => {
        await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY));
        return createCompletionStream(model, messages, ticket, refConvId, retryCount + 1, tools, toolChoice);
      })();
    }
    throw err;
  });
}

async function generateImages(
  model = MODEL_NAME,
  prompt: string,
  ticket: string,
  retryCount = 0
) {
  let session: http2.ClientHttp2Session;
  return (async () => {
    const messages = [
      { role: "user", content: prompt.indexOf('画') == -1 ? `请画：${prompt}` : prompt },
    ];
    // 请求流
    const session: http2.ClientHttp2Session = await new Promise(
      (resolve, reject) => {
        const session = http2.connect("https://qianwen.biz.aliyun.com");
        session.on("connect", () => resolve(session));
        session.on("error", reject);
      }
    );
    const req = session.request({
      ":method": "POST",
      ":path": "/dialog/conversation",
      "Content-Type": "application/json",
      Cookie: generateCookie(ticket),
      ...FAKE_HEADERS,
      Accept: "text/event-stream",
    });
    req.setTimeout(120000);
    req.write(
      JSON.stringify({
        mode: "chat",
        model: "",
        action: "next",
        userAction: "chat",
        requestId: util.uuid(false),
        sessionId: "",
        sessionType: "text_chat",
        parentMsgId: "",
        params: {
          "fileUploadBatchId": util.uuid()
        },
        contents: messagesPrepare(messages),
      })
    );
    req.setEncoding("utf8");
    const streamStartTime = util.timestamp();
    // 接收流为输出文本
    const { convId, imageUrls } = await receiveImages(req);
    session.close();
    logger.success(
      `Stream has completed transfer ${util.timestamp() - streamStartTime}ms`
    );

    // 异步移除会话，如果消息不合规，此操作可能会抛出数据库错误异常，请忽略
    removeConversation(convId, ticket).catch((err) => console.error(err));

    return imageUrls;
  })().catch((err) => {
    session && session.close();
    if (retryCount < MAX_RETRY_COUNT) {
      logger.error(`Stream response error: ${err.message}`);
      logger.warn(`Try again after ${RETRY_DELAY / 1000}s...`);
      return (async () => {
        await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY));
        return generateImages(model, prompt, ticket, retryCount + 1);
      })();
    }
    throw err;
  });
}

/**
 * 解析文本中的工具调用
 * 
 * @param text 文本内容
 * @returns 解析结果 { toolCalls: 工具调用数组, cleanedText: 清理后的文本 }
 */
function parseToolCallsFromText(text: string): { toolCalls: any[], cleanedText: string } {
  const toolCalls: any[] = [];
  let cleanedText = text;
  
  logger.info(`[parseToolCallsFromText] 输入文本长度: ${text.length}, 前200字符: ${text.substring(0, 200)}`);
  
  // 使用括号计数法查找所有 TOOL_CALL: 的位置（支持任意深度嵌套）
  const toolCallPrefix = 'TOOL_CALL:';
  let startIndex = 0;
  
  while ((startIndex = text.indexOf(toolCallPrefix, startIndex)) !== -1) {
    // 跳过 "TOOL_CALL:" 前缀
    let jsonStart = startIndex + toolCallPrefix.length;
    
    // 跳过空白字符
    while (jsonStart < text.length && /\s/.test(text[jsonStart])) {
      jsonStart++;
    }
    
    // 确保是 JSON 对象开始
    if (jsonStart >= text.length || text[jsonStart] !== '{') {
      startIndex = jsonStart;
      continue;
    }
    
    // 使用括号计数法找到完整的 JSON 对象
    let braceCount = 0;
    let jsonEnd = jsonStart;
    let inString = false;
    let escapeNext = false;
    
    for (let i = jsonStart; i < text.length; i++) {
      const char = text[i];
      
      if (escapeNext) {
        escapeNext = false;
        continue;
      }
      
      if (char === '\\') {
        escapeNext = true;
        continue;
      }
      
      if (char === '"') {
        inString = !inString;
        continue;
      }
      
      if (!inString) {
        if (char === '{') {
          braceCount++;
        } else if (char === '}') {
          braceCount--;
          if (braceCount === 0) {
            jsonEnd = i + 1;
            break;
          }
        }
      }
    }
    
    // 提取 JSON 字符串
    const jsonStr = text.substring(jsonStart, jsonEnd);
    const fullMatch = text.substring(startIndex, jsonEnd);
    
    logger.info(`[parseToolCallsFromText] 找到匹配，长度: ${fullMatch.length}, 前100字符: ${fullMatch.substring(0, 100)}...`);
    
    try {
      logger.info(`[parseToolCallsFromText] 尝试解析 JSON (长度: ${jsonStr.length})`);
      const toolCallData = JSON.parse(jsonStr);
      logger.info(`[parseToolCallsFromText] JSON 解析成功: ${JSON.stringify(toolCallData).substring(0, 200)}`);
      
      if (toolCallData.name && toolCallData.arguments !== undefined) {
        toolCalls.push({
          id: `call_${util.uuid(false)}`,
          type: 'function',
          function: {
            name: toolCallData.name,
            arguments: typeof toolCallData.arguments === 'string' 
              ? toolCallData.arguments 
              : JSON.stringify(toolCallData.arguments)
          }
        });
        // 从文本中移除工具调用标记
        cleanedText = cleanedText.replace(fullMatch, '').trim();
        logger.info(`[parseToolCallsFromText] 成功添加工具调用: ${toolCallData.name}`);
      } else {
        logger.warn(`[parseToolCallsFromText] 工具调用数据不完整: name=${toolCallData.name}, arguments=${toolCallData.arguments}`);
      }
    } catch (err) {
      logger.error(`[parseToolCallsFromText] JSON 解析失败: ${err.message}`);
      logger.error(`[parseToolCallsFromText] 失败的 JSON (长度: ${jsonStr.length}): ${jsonStr.substring(0, 200)}`);
    }
    
    // 移动到下一个可能的位置
    startIndex = jsonEnd > startIndex ? jsonEnd : startIndex + 1;
  }
  
  logger.info(`[parseToolCallsFromText] 总共解析出 ${toolCalls.length} 个工具调用`);
  
  return { toolCalls, cleanedText };
}

/**
 * 提取消息中引用的文件URL
 *
 * @param messages 参考gpt系列消息格式，多轮对话请完整提供上下文
 */
function extractRefFileUrls(messages: any[]) {
  const urls = [];
  // 如果没有消息，则返回[]
  if (!messages.length) {
    return urls;
  }
  // 只获取最新的消息
  const lastMessage = messages[messages.length - 1];
  if (_.isArray(lastMessage.content)) {
    lastMessage.content.forEach((v) => {
      if (!_.isObject(v) || !["file", "image_url"].includes(v["type"])) return;
      // glm-free-api支持格式
      if (
        v["type"] == "file" &&
        _.isObject(v["file_url"]) &&
        _.isString(v["file_url"]["url"])
      )
        urls.push(v["file_url"]["url"]);
      // 兼容gpt-4-vision-preview API格式
      else if (
        v["type"] == "image_url" &&
        _.isObject(v["image_url"]) &&
        _.isString(v["image_url"]["url"])
      )
        urls.push(v["image_url"]["url"]);
    });
  }
  logger.info("本次请求上传：" + urls.length + "个文件");
  return urls;
}

/**
 * 消息预处理
 *
 * 由于接口只取第一条消息，此处会将多条消息合并为一条，实现多轮对话效果
 * user:旧消息1
 * assistant:旧消息2
 * user:新消息
 *
 * @param messages 参考gpt系列消息格式，多轮对话请完整提供上下文
 * @param refs 参考文件列表
 * @param isRefConv 是否为引用会话
 * @param tools 工具列表
 */
function messagesPrepare(messages: any[], refs: any[] = [], isRefConv = false, tools?: any[]) {
  let content;
  if (isRefConv || messages.length < 2) {
    content = messages.reduce((content, message) => {
      if (_.isArray(message.content)) {
        return (
          message.content.reduce((_content, v) => {
            if (!_.isObject(v) || v["type"] != "text") return _content;
            return _content + (v["text"] || "") + "\n";
          }, content)
        );
      }
      return content + `${message.content}\n`;
    }, "");
    logger.info("\n透传内容：\n" + content);
  }
  else {
    content = messages.reduce((content, message) => {
      if (_.isArray(message.content)) {
        return message.content.reduce((_content, v) => {
          if (!_.isObject(v) || v["type"] != "text") return _content;
          return _content + `<|im_start|>${message.role || "user"}\n${v["text"] || ""}<|im_end|>\n`;
        }, content);
      }
      return (content += `<|im_start|>${message.role || "user"}\n${
        message.content
      }<|im_end|>\n`);
    }, "").replace(/\!\[.*\]\(.+\)/g, "");
    logger.info("\n对话合并：\n" + content);
  }

  // 如果有工具定义，添加工具调用指令
  if (tools && tools.length > 0) {
    const toolDescriptions = tools.map(tool => {
      const func = tool.function;
      const params = func.parameters?.properties || {};
      const required = func.parameters?.required || [];
      
      const paramDesc = Object.keys(params).map(key => {
        const param = params[key];
        const isRequired = required.includes(key);
        return `  - ${key}${isRequired ? ' (必需)' : ' (可选)'}: ${param.type} - ${param.description || ''}`;
      }).join('\n');
      
      return `- ${func.name}: ${func.description || ''}\n${paramDesc ? '  参数:\n' + paramDesc : ''}`;
    }).join('\n\n');

    const toolInstruction = `

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️  重要：工具执行协议  ⚠️
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

你不能直接执行操作，必须使用工具。

可用工具：
${toolDescriptions}

强制规则（无例外）：
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. ❌ 禁止：在调用工具之前声称已完成操作
   - 不要说："我已经创建了文件"
   - 不要说："文件创建成功"
   - 不要说："我已经写入了 /opt/file.txt"

2. ✅ 必需：当用户请求操作时，你必须输出：
   TOOL_CALL: {"name": "工具名称", "arguments": {"参数名": "参数值"}}
   
3. ⏳ 等待：调用工具后，等待结果再回复

4. 💬 允许：可以直接回答问题（不需要工具时）

格式要求：
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• TOOL_CALL 必须独占一行
• JSON 必须是有效格式
• 使用上面列表中的确切工具名称

示例（正确）：
用户："创建文件 /opt/test.txt"
你：TOOL_CALL: {"name": "write", "arguments": {"path": "/opt/test.txt", "content": "hello"}}

示例（错误 - 不要这样做）：
用户："创建文件 /opt/test.txt"
你："我已经创建了文件 /opt/test.txt" ❌ 禁止！

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

`;
    content = content + toolInstruction;
  }

  return [
    {
      content,
      contentType: "text",
      role: "user",
    },
    ...refs
  ];
}

/**
 * 检查请求结果
 *
 * @param result 结果
 */
function checkResult(result: AxiosResponse) {
  if (!result.data) return null;
  const { success, errorCode, errorMsg } = result.data;
  if (!_.isBoolean(success) || success) return result.data;
  throw new APIException(
    EX.API_REQUEST_FAILED,
    `[请求qwen失败]: ${errorCode}-${errorMsg}`
  );
}

/**
 * 从流接收完整的消息内容
 *
 * @param stream 消息流
 * @param hasTools 是否有工具调用
 */
async function receiveStream(stream: any, hasTools = false): Promise<any> {
  return new Promise((resolve, reject) => {
    // 消息初始化
    const data = {
      id: "",
      model: MODEL_NAME,
      object: "chat.completion",
      choices: [
        {
          index: 0,
          message: { 
            role: "assistant", 
            content: "",
            tool_calls: undefined as any[] | undefined
          },
          finish_reason: "stop",
        },
      ],
      usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
      created: util.unixTimestamp(),
    };
    
    // 工具调用相关
    let toolCalls: any[] = [];
    
    const parser = createParser((event) => {
      try {
        if (event.type !== "event") return;
        if (event.data == "[DONE]") return;
        // 过滤心跳消息
        if (event.data == "[heartbeat]") return;
        // 解析JSON
        const result = _.attempt(() => JSON.parse(event.data));
        if (_.isError(result))
          throw new Error(`Stream response invalid: ${event.data}`);
        if (!data.id && result.sessionId && result.msgId)
          data.id = `${result.sessionId}-${result.msgId}`;
        const text = (result.contents || []).reduce((str, part) => {
          const { contentType, role, content } = part;
          if (contentType != "text" && contentType != "text2image") return str;
          if (role != "assistant" && !_.isString(content)) return str;
          return str + content;
        }, "");
        const exceptCharIndex = text.indexOf("�");
        let chunk = text.substring(
          exceptCharIndex != -1
            ? Math.min(data.choices[0].message.content.length, exceptCharIndex)
            : data.choices[0].message.content.length,
          exceptCharIndex == -1 ? text.length : exceptCharIndex
        );
        if (chunk && result.contentType == "text2image") {
          chunk = chunk.replace(
            /https?:\/\/[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=\,]*)/gi,
            (url) => {
              const urlObj = new URL(url);
              urlObj.search = "";
              return urlObj.toString();
            }
          );
        }
        if (result.msgStatus != "finished") {
          if (result.contentType == "text")
            data.choices[0].message.content += chunk;
        } else {
          data.choices[0].message.content += chunk;
          if (!result.canShare)
            data.choices[0].message.content +=
              "\n[内容由于不合规被停止生成，我们换个话题吧]";
          if (result.errorCode)
            data.choices[0].message.content += `服务暂时不可用，第三方响应错误：${result.errorCode}`;
          
          let finalContent = data.choices[0].message.content;
          
          logger.info(`[工具调用] hasTools: ${hasTools}, toolCalls.length: ${toolCalls.length}`);
          logger.info(`[工具调用] finalContent: ${finalContent.substring(0, 200)}`);
          
          // 如果启用了工具调用，尝试从文本中解析工具调用
          if (hasTools && toolCalls.length === 0) {
            logger.info('[工具调用] 开始解析文本中的工具调用');
            const parsed = parseToolCallsFromText(finalContent);
            logger.info(`[工具调用] 解析结果: ${parsed.toolCalls.length} 个工具调用`);
            if (parsed.toolCalls.length > 0) {
              logger.info(`[工具调用] 工具调用详情: ${JSON.stringify(parsed.toolCalls)}`);
              toolCalls = parsed.toolCalls;
              finalContent = parsed.cleanedText;
            }
          }
          
          data.choices[0].message.content = finalContent;
          
          // 添加工具调用到消息中
          if (toolCalls.length > 0) {
            data.choices[0].message.tool_calls = toolCalls;
            data.choices[0].finish_reason = 'tool_calls';
            logger.success('[工具调用] 成功设置 tool_calls');
          }
          
          resolve(data);
        }
      } catch (err) {
        logger.error(err);
        reject(err);
      }
    });
    // 将流数据喂给SSE转换器
    stream.on("data", (buffer) => parser.feed(buffer.toString()));
    stream.once("error", (err) => reject(err));
    stream.once("close", () => {
      // 流结束时，如果启用了工具调用，尝试从文本中解析
      if (hasTools && toolCalls.length === 0) {
        logger.info(`[工具调用] 流结束，开始解析文本中的工具调用`);
        logger.info(`[工具调用] 最终内容: ${data.choices[0].message.content.substring(0, 300)}`);
        const parsed = parseToolCallsFromText(data.choices[0].message.content);
        if (parsed.toolCalls.length > 0) {
          logger.success(`[工具调用] 成功解析 ${parsed.toolCalls.length} 个工具调用`);
          toolCalls = parsed.toolCalls;
          data.choices[0].message.content = parsed.cleanedText;
          data.choices[0].message.tool_calls = toolCalls;
          data.choices[0].finish_reason = 'tool_calls';
        } else {
          logger.warn(`[工具调用] 未能解析出工具调用`);
        }
      }
      resolve(data);
    });
    stream.end();
  });
}

/**
 * 创建转换流
 *
 * 将流格式转换为gpt兼容流格式
 *
 * @param stream 消息流
 * @param hasTools 是否有工具调用
 * @param endCallback 传输结束回调
 */
function createTransStream(stream: any, hasTools = false, endCallback?: Function) {
  // 消息创建时间
  const created = util.unixTimestamp();
  // 创建转换流
  const transStream = new PassThrough();
  let content = "";
  
  // 工具调用相关
  let toolCalls: any[] = [];
  let accumulatedContent = ''; // 累积的内容，用于解析工具调用
  
  !transStream.closed &&
    transStream.write(
      `data: ${JSON.stringify({
        id: "",
        model: MODEL_NAME,
        object: "chat.completion.chunk",
        choices: [
          {
            index: 0,
            delta: { role: "assistant", content: "" },
            finish_reason: null,
          },
        ],
        created,
      })}\n\n`
    );
  const parser = createParser((event) => {
    try {
      if (event.type !== "event") return;
      if (event.data == "[DONE]") return;
      // 过滤心跳消息
      if (event.data == "[heartbeat]") return;
      // 解析JSON
      const result = _.attempt(() => JSON.parse(event.data));
      if (_.isError(result))
        throw new Error(`Stream response invalid: ${event.data}`);
      const text = (result.contents || []).reduce((str, part) => {
        const { contentType, role, content } = part;
        if (contentType != "text" && contentType != "text2image") return str;
        if (role != "assistant" && !_.isString(content)) return str;
        return str + content;
      }, "");
      const exceptCharIndex = text.indexOf("�");
      let chunk = text.substring(
        exceptCharIndex != -1
          ? Math.min(content.length, exceptCharIndex)
          : content.length,
        exceptCharIndex == -1 ? text.length : exceptCharIndex
      );
      if (chunk && result.contentType == "text2image") {
        chunk = chunk.replace(
          /https?:\/\/[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=\,]*)/gi,
          (url) => {
            const urlObj = new URL(url);
            urlObj.search = "";
            return urlObj.toString();
          }
        );
      }
      if (result.msgStatus != "finished") {
        if (chunk && result.contentType == "text") {
          content += chunk;
          
          // 累积内容用于工具调用检测
          if (hasTools) {
            accumulatedContent += chunk;
            
            // 检查是否包含完整的工具调用（支持嵌套 JSON）
            const toolCallMatch = accumulatedContent.match(/TOOL_CALL:\s*(\{(?:[^{}]|\{[^{}]*\})*\})/);
            if (toolCallMatch) {
              try {
                const toolCallData = JSON.parse(toolCallMatch[1]);
                if (toolCallData.name && toolCallData.arguments !== undefined) {
                  const toolCall = {
                    id: `call_${util.uuid(false)}`,
                    type: 'function',
                    function: {
                      name: toolCallData.name,
                      arguments: typeof toolCallData.arguments === 'string' 
                        ? toolCallData.arguments 
                        : JSON.stringify(toolCallData.arguments)
                    }
                  };
                  toolCalls.push(toolCall);
                  
                  // 发送工具调用
                  transStream.write(`data: ${JSON.stringify({
                    id: `${result.sessionId}-${result.msgId}`,
                    model: MODEL_NAME,
                    object: "chat.completion.chunk",
                    choices: [
                      {
                        index: 0,
                        delta: {
                          tool_calls: [{
                            index: toolCalls.length - 1,
                            id: toolCall.id,
                            type: 'function',
                            function: {
                              name: toolCall.function.name,
                              arguments: toolCall.function.arguments
                            }
                          }]
                        },
                        finish_reason: null,
                      },
                    ],
                    created,
                  })}\n\n`);
                  
                  // 清除已处理的工具调用部分
                  accumulatedContent = accumulatedContent.replace(toolCallMatch[0], '').trim();
                  return; // 不发送包含 TOOL_CALL 的内容
                }
              } catch (err) {
                // JSON 解析失败，继续累积
              }
            }
          }
          
          const data = `data: ${JSON.stringify({
            id: `${result.sessionId}-${result.msgId}`,
            model: MODEL_NAME,
            object: "chat.completion.chunk",
            choices: [
              { index: 0, delta: { content: chunk }, finish_reason: null },
            ],
            created,
          })}\n\n`;
          !transStream.closed && transStream.write(data);
        }
      } else {
        // 在流式响应结束时，如果还有累积的内容未解析，尝试解析工具调用
        if (hasTools && toolCalls.length === 0 && accumulatedContent.trim()) {
          logger.info(`[流式工具调用] 结束时检查累积内容: ${accumulatedContent.substring(0, 200)}`);
          const toolCallMatch = accumulatedContent.match(/TOOL_CALL:\s*(\{(?:[^{}]|\{[^{}]*\})*\})/);
          if (toolCallMatch) {
            try {
              const toolCallData = JSON.parse(toolCallMatch[1]);
              if (toolCallData.name && toolCallData.arguments !== undefined) {
                const toolCall = {
                  id: `call_${util.uuid(false)}`,
                  type: 'function',
                  function: {
                    name: toolCallData.name,
                    arguments: typeof toolCallData.arguments === 'string' 
                      ? toolCallData.arguments 
                      : JSON.stringify(toolCallData.arguments)
                  }
                };
                toolCalls.push(toolCall);
                logger.success(`[流式工具调用] 在结束时成功解析工具调用: ${toolCallData.name}`);
                
                // 发送工具调用
                transStream.write(`data: ${JSON.stringify({
                  id: `${result.sessionId}-${result.msgId}`,
                  model: MODEL_NAME,
                  object: "chat.completion.chunk",
                  choices: [
                    {
                      index: 0,
                      delta: {
                        tool_calls: [{
                          index: 0,
                          id: toolCall.id,
                          type: 'function',
                          function: {
                            name: toolCall.function.name,
                            arguments: toolCall.function.arguments
                          }
                        }]
                      },
                      finish_reason: null,
                    },
                  ],
                  created,
                })}\n\n`);
              }
            } catch (err) {
              logger.warn(`[流式工具调用] 结束时解析失败: ${err.message}`);
            }
          }
        }
        
        const finishReason = toolCalls.length > 0 ? 'tool_calls' : 'stop';
        logger.info(`[流式工具调用] 发送结束标记, finishReason: ${finishReason}, toolCalls: ${toolCalls.length}`);
        
        const delta = { content: chunk || "" };
        if (!result.canShare)
          delta.content += "\n[内容由于不合规被停止生成，我们换个话题吧]";
        if (result.errorCode)
          delta.content += `服务暂时不可用，第三方响应错误：${result.errorCode}`;
        const data = `data: ${JSON.stringify({
          id: `${result.sessionId}-${result.msgId}`,
          model: MODEL_NAME,
          object: "chat.completion.chunk",
          choices: [
            {
              index: 0,
              delta,
              finish_reason: finishReason,
            },
          ],
          usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
          created,
        })}\n\n`;
        !transStream.closed && transStream.write(data);
        !transStream.closed && transStream.end("data: [DONE]\n\n");
        content = "";
        endCallback && endCallback(result.sessionId);
      }
      // else
      //   logger.warn(result.event, result);
    } catch (err) {
      logger.error(err);
      !transStream.closed && transStream.end("\n\n");
    }
  });
  // 将流数据喂给SSE转换器
  stream.on("data", (buffer) => parser.feed(buffer.toString()));
  stream.once(
    "error",
    () => !transStream.closed && transStream.end("data: [DONE]\n\n")
  );
  stream.once(
    "close",
    () => !transStream.closed && transStream.end("data: [DONE]\n\n")
  );
  stream.end();
  return transStream;
}

/**
 * 从流接收图像
 *
 * @param stream 消息流
 */
async function receiveImages(
  stream: any
): Promise<{ convId: string; imageUrls: string[] }> {
  return new Promise((resolve, reject) => {
    let convId = "";
    const imageUrls = [];
    const parser = createParser((event) => {
      try {
        if (event.type !== "event") return;
        if (event.data == "[DONE]") return;
        // 过滤心跳消息
        if (event.data == "[heartbeat]") return;
        // 解析JSON
        const result = _.attempt(() => JSON.parse(event.data));
        if (_.isError(result))
          throw new Error(`Stream response invalid: ${event.data}`);
        if (!convId && result.sessionId) convId = result.sessionId;
        const text = (result.contents || []).reduce((str, part) => {
          const { role, content } = part;
          if (role != "assistant" && !_.isString(content)) return str;
          return str + content;
        }, "");
        if (result.contentFrom == "text2image") {
          const urls =
            text.match(
              /https?:\/\/[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=\,]*)/gi
            ) || [];
          urls.forEach((url) => {
            const urlObj = new URL(url);
            urlObj.search = "";
            const imageUrl = urlObj.toString();
            if (imageUrls.indexOf(imageUrl) != -1) return;
            imageUrls.push(imageUrl);
          });
        }
        if (result.msgStatus == "finished") {
          if (!result.canShare || imageUrls.length == 0)
            throw new APIException(EX.API_CONTENT_FILTERED);
          if (result.errorCode)
            throw new APIException(
              EX.API_REQUEST_FAILED,
              `服务暂时不可用，第三方响应错误：${result.errorCode}`
            );
        }
      } catch (err) {
        logger.error(err);
        reject(err);
      }
    });
    // 将流数据喂给SSE转换器
    stream.on("data", (buffer) => parser.feed(buffer.toString()));
    stream.once("error", (err) => reject(err));
    stream.once("close", () => resolve({ convId, imageUrls }));
    stream.end();
  });
}

/**
 * 获取上传参数
 *
 * @param ticket tongyi_sso_ticket或login_aliyunid_ticket
 */
async function acquireUploadParams(ticket: string) {
  const result = await axios.post(
    "https://qianwen.biz.aliyun.com/dialog/uploadToken",
    {},
    {
      timeout: 15000,
      headers: {
        Cookie: generateCookie(ticket),
        ...FAKE_HEADERS,
      },
      validateStatus: () => true,
    }
  );
  const { data } = checkResult(result);
  return data;
}

/**
 * 预检查文件URL有效性
 *
 * @param fileUrl 文件URL
 */
async function checkFileUrl(fileUrl: string) {
  if (util.isBASE64Data(fileUrl)) return;
  const result = await axios.head(fileUrl, {
    timeout: 15000,
    validateStatus: () => true,
  });
  if (result.status >= 400)
    throw new APIException(
      EX.API_FILE_URL_INVALID,
      `File ${fileUrl} is not valid: [${result.status}] ${result.statusText}`
    );
  // 检查文件大小
  if (result.headers && result.headers["content-length"]) {
    const fileSize = parseInt(result.headers["content-length"], 10);
    if (fileSize > FILE_MAX_SIZE)
      throw new APIException(
        EX.API_FILE_EXECEEDS_SIZE,
        `File ${fileUrl} is not valid`
      );
  }
}

/**
 * 上传文件
 *
 * @param fileUrl 文件URL
 * @param ticket tongyi_sso_ticket或login_aliyunid_ticket
 */
async function uploadFile(fileUrl: string, ticket: string) {
  // 预检查远程文件URL可用性
  await checkFileUrl(fileUrl);

  let filename, fileData, mimeType;
  // 如果是BASE64数据则直接转换为Buffer
  if (util.isBASE64Data(fileUrl)) {
    mimeType = util.extractBASE64DataFormat(fileUrl);
    const ext = mime.getExtension(mimeType);
    filename = `${util.uuid()}.${ext}`;
    fileData = Buffer.from(util.removeBASE64DataHeader(fileUrl), "base64");
  }
  // 下载文件到内存，如果您的服务器内存很小，建议考虑改造为流直传到下一个接口上，避免停留占用内存
  else {
    filename = path.basename(fileUrl);
    ({ data: fileData } = await axios.get(fileUrl, {
      responseType: "arraybuffer",
      // 100M限制
      maxContentLength: FILE_MAX_SIZE,
      // 60秒超时
      timeout: 60000,
    }));
  }

  // 获取文件的MIME类型
  mimeType = mimeType || mime.getType(filename);

  // 获取上传参数
  const { accessId, policy, signature, dir } = await acquireUploadParams(
    ticket
  );

  const formData = new FormData();
  formData.append("OSSAccessKeyId", accessId);
  formData.append("policy", policy);
  formData.append("signature", signature);
  formData.append("key", `${dir}${filename}`);
  formData.append("dir", dir);
  formData.append("success_action_status", "200");
  formData.append("file", fileData, {
    filename,
    contentType: mimeType,
  });

  // 上传文件到OSS
  await axios.request({
    method: "POST",
    url: "https://broadscope-dialogue.oss-cn-beijing.aliyuncs.com/",
    data: formData,
    // 100M限制
    maxBodyLength: FILE_MAX_SIZE,
    // 60秒超时
    timeout: 120000,
    headers: {
      ...FAKE_HEADERS,
      "X-Requested-With": "XMLHttpRequest"
    }
  });

  const isImage = [
    'image/jpeg',
    'image/jpg',
    'image/tiff',
    'image/png',
    'image/bmp',
    'image/gif',
    'image/svg+xml', 
    'image/webp',
    'image/ico',
    'image/heic',
    'image/heif',
    'image/bmp',
    'image/x-icon',
    'image/vnd.microsoft.icon',
    'image/x-png'
  ].includes(mimeType);

  if(isImage) {
    const result = await axios.post(
      "https://qianwen.biz.aliyun.com/dialog/downloadLink",
      {
        fileKey: filename,
        fileType: "image",
        dir
      },
      {
        timeout: 15000,
        headers: {
          Cookie: generateCookie(ticket),
          ...FAKE_HEADERS,
        },
        validateStatus: () => true,
      }
    );
    const { data } = checkResult(result);
    return {
      role: "user",
      contentType: "image",
      content: data.url
    };
  }
  else {
    let result = await axios.post(
      "https://qianwen.biz.aliyun.com/dialog/downloadLink/batch",
      {
        fileKeys: [filename],
        fileType: "file",
        dir
      },
      {
        timeout: 15000,
        headers: {
          Cookie: generateCookie(ticket),
          ...FAKE_HEADERS,
        },
        validateStatus: () => true,
      }
    );
    const { data } = checkResult(result);
    if(!data.results[0] || !data.results[0].url)
      throw new Error(`文件上传失败：${data.results[0] ? data.results[0].errorMsg : '未知错误'}`);
    const url = data.results[0].url;
    const startTime = util.timestamp();
    while(true) {
      result = await axios.post(
        "https://qianwen.biz.aliyun.com/dialog/secResult/batch",
        {
          urls: [url]
        },
        {
          timeout: 15000,
          headers: {
            Cookie: generateCookie(ticket),
            ...FAKE_HEADERS,
          },
          validateStatus: () => true,
        }
      );
      const { data } = checkResult(result);
      if(data.pollEndFlag) {
        if(data.statusList[0] && data.statusList[0].status === 0)
          throw new Error(`文件处理失败：${data.statusList[0].errorMsg || '未知错误'}`);
        break;
      }
      if(util.timestamp() > startTime + 120000)
        throw new Error("文件处理超时：超出120秒");
    }
    return {
      role: "user",
      contentType: "file",
      content: url,
      ext: { fileSize: fileData.byteLength }
    };
  }
}

/**
 * Token切分
 *
 * @param authorization 认证字符串
 */
function tokenSplit(authorization: string) {
  return authorization.replace("Bearer ", "").split(",");
}

/**
 * 生成Cookies
 *
 * @param ticket tongyi_sso_ticket或login_aliyunid_ticket
 */
function generateCookie(ticket: string) {
  return [
    `${ticket.length > 100 ? 'login_aliyunid_ticket' : 'tongyi_sso_ticket'}=${ticket}`,
    'aliyun_choice=intl',
    "_samesite_flag_=true",
    `t=${util.uuid(false)}`,
    // `login_aliyunid_csrf=_csrf_tk_${util.generateRandomString({ charset: 'numeric', length: 15 })}`,
    // `cookie2=${util.uuid(false)}`,
    // `munb=22${util.generateRandomString({ charset: 'numeric', length: 11 })}`,
    // `csg=`,
    // `_tb_token_=${util.generateRandomString({ length: 10, capitalization: 'lowercase' })}`,
    // `cna=`,
    // `cnaui=`,
    // `atpsida=`,
    // `isg=`,
    // `tfstk=`,
    // `aui=`,
    // `sca=`
  ].join("; ");
}

/**
 * 获取Token存活状态
 */
async function getTokenLiveStatus(ticket: string) {
  const result = await axios.post(
    "https://qianwen.biz.aliyun.com/dialog/session/list",
    {},
    {
      headers: {
        Cookie: generateCookie(ticket),
        ...FAKE_HEADERS,
      },
      timeout: 15000,
      validateStatus: () => true,
    }
  );
  try {
    const { data } = checkResult(result);
    return _.isArray(data);
  }
  catch(err) {
    return false;
  }
}

export default {
  createCompletion,
  createCompletionStream,
  generateImages,
  getTokenLiveStatus,
  tokenSplit,
};
