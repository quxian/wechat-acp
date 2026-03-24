# wechat-acp 核心 ACP 交互链路

> 本文件将分散在 `src/acp/`、`src/adapter/`、`src/bridge.ts`、`src/config.ts` 中的核心逻辑
> **浓缩提炼**为一个自上而下可读的完整流程。阅读本文件即可理解 wechat-acp 如何与
> Copilot（或其他 ACP Agent）完成全链路交互。

---

```typescript
/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║              wechat-acp 整体数据流图                                      ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║                                                                          ║
 * ║  微信用户                                                                 ║
 * ║    │  发送消息（文本/图片/语音/文件/视频）                                  ║
 * ║    ▼                                                                     ║
 * ║  [微信 iLink 服务端]                                                      ║
 * ║    │  HTTP 长轮询（/cgi-bin/sync_msg）                                    ║
 * ║    ▼                                                                     ║
 * ║  startMonitor()  ──→  onMessage(WeixinMessage)                           ║
 * ║    │                                                                     ║
 * ║    │  ① 消息格式转换（入站）                                               ║
 * ║    ▼                                                                     ║
 * ║  weixinMessageToPrompt()  →  acp.ContentBlock[]                          ║
 * ║    │   文本块 { type:"text", text }                                       ║
 * ║    │   图片块 { type:"image", data:base64, mimeType }                     ║
 * ║    │   文件块 { type:"resource", resource:{ uri, text } }                 ║
 * ║    │                                                                     ║
 * ║    │  ② 多用户会话管理                                                    ║
 * ║    ▼                                                                     ║
 * ║  SessionManager.enqueue(userId, { prompt, contextToken })                ║
 * ║    │   per-user queue（串行处理，防止乱序）                                 ║
 * ║    │   超出 maxConcurrentUsers → 驱逐最久未活跃的会话                       ║
 * ║    │                                                                     ║
 * ║    │  ③ Agent 进程启动（首次消息时惰性创建）                                ║
 * ║    ▼                                                                     ║
 * ║  spawnAgent()                                                            ║
 * ║    │  spawn(command, args, { stdio:["pipe","pipe","inherit"] })           ║
 * ║    │  ndJsonStream(stdin, stdout)  ── NDJSON 帧格式化                     ║
 * ║    │  ClientSideConnection(client, stream)                               ║
 * ║    │  connection.initialize()  ── ACP 握手                               ║
 * ║    │  connection.newSession()  ── 获得 sessionId                         ║
 * ║    │                                                                     ║
 * ║    │  ④ ACP Client 回调（Agent → 客户端）                                 ║
 * ║    │   WeChatAcpClient.sessionUpdate()  ── 累积 text chunk               ║
 * ║    │   WeChatAcpClient.requestPermission()  ── 自动批准                   ║
 * ║    │   WeChatAcpClient.readTextFile()  ── 文件系统代理（读）               ║
 * ║    │   WeChatAcpClient.writeTextFile()  ── 文件系统代理（写）              ║
 * ║    │                                                                     ║
 * ║    │  ⑤ 发送 prompt 并收集回复                                            ║
 * ║    ▼                                                                     ║
 * ║  connection.prompt({ sessionId, prompt })  →  等待 Agent 完成             ║
 * ║    │  流式 chunk 在 sessionUpdate 中被 WeChatAcpClient 累积               ║
 * ║    │  Agent 结束后调用 client.flush() 取出完整回复文本                     ║
 * ║    │                                                                     ║
 * ║    │  ⑥ 消息格式转换（出站）                                               ║
 * ║    ▼                                                                     ║
 * ║  formatForWeChat()  →  去除 Markdown 标记 → 纯文本                        ║
 * ║    │  splitText(text, 4000)  →  分段（微信单条消息限长）                    ║
 * ║    ▼                                                                     ║
 * ║  sendTextMessage()  →  HTTP POST  →  微信用户收到回复                     ║
 * ║                                                                          ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */

import { spawn, type ChildProcess } from "node:child_process";
import { Writable, Readable } from "node:stream";
import * as acp from "@agentclientprotocol/sdk";
import fs from "node:fs";

// ─────────────────────────────────────────────────────────────────────────────
// 类型声明（精简自 src/config.ts 和 src/weixin/types.ts）
// ─────────────────────────────────────────────────────────────────────────────

interface WeChatAcpConfig {
  agent:   { command: string; args: string[]; cwd: string; env?: Record<string, string> };
  session: { idleTimeoutMs: number; maxConcurrentUsers: number };
  wechat:  { baseUrl: string; cdnBaseUrl: string; botType: string };
  storage: { dir: string };
}

interface TokenData { baseUrl: string; token: string; accountId: string; savedAt: string }

// 微信消息体（节选核心字段）
interface WeixinMessage {
  message_type: number;   // 1=用户消息；其他=系统消息
  group_id?: string;      // 群消息时有值；私聊为空
  from_user_id: string;   // 发消息的用户 ID
  context_token: string;  // 用于后续 API 调用的上下文凭证
  item_list?: MessageItem[];
}

// ─────────────────────────────────────────────────────────────────────────────
// 零、入口：main() ── 自上而下串联所有阶段
// ─────────────────────────────────────────────────────────────────────────────

/**
 * 程序入口。所有核心逻辑都从这里展开。
 * 对应 src/bridge.ts WeChatAcpBridge.start()。
 */
async function main() {
  // ① 加载配置（agent 命令、工作目录、会话参数等）
  const config: WeChatAcpConfig = loadConfig();

  // ② 微信扫码登录，或加载磁盘上已保存的 token（避免每次都要扫码）
  const tokenData: TokenData = await loginOrLoadToken(config);

  // ③ 创建多用户会话管理器，并注册回调：
  //    · onReply   —— Agent 回复时，格式化后发回微信
  //    · sendTyping —— Agent 处理中时，向用户发"对方正在输入"状态
  const sessionManager = new SessionManager({
    agentCommand:       config.agent.command,  // e.g. "npx"
    agentArgs:          config.agent.args,     // e.g. ["@github/copilot","--acp","--yolo"]
    agentCwd:           config.agent.cwd,      // 工作目录，Agent 在此目录下读写文件
    agentEnv:           config.agent.env,
    idleTimeoutMs:      config.session.idleTimeoutMs,       // 30 分钟无活动释放
    maxConcurrentUsers: config.session.maxConcurrentUsers,  // 默认最多 10 个并发用户

    onReply: async (userId, contextToken, text) => {
      // 出站格式转换：去除 Markdown 标记 → 微信友好纯文本
      const formatted = formatForWeChat(text);
      // 超过 4000 字符时分段发送（微信 API 限制）
      const segments = splitText(formatted, 4000);
      for (const seg of segments) {
        await sendTextMessage(userId, seg, { baseUrl: tokenData.baseUrl, token: tokenData.token, contextToken });
      }
    },

    sendTyping: (userId, contextToken) =>
      sendTypingIndicator(userId, contextToken, tokenData),
  });
  sessionManager.start();  // 启动空闲会话清理定时器（每 2 分钟扫一次）

  // ④ 启动微信消息长轮询（会一直阻塞到 AbortController.abort()）
  await startMonitor({
    baseUrl:     tokenData.baseUrl,
    token:       tokenData.token,
    abortSignal: abortController.signal,
    onMessage:   (msg) => handleMessage(msg, sessionManager, config),
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// 一、消息入口：微信消息 → ACP prompt → 入队
// ─────────────────────────────────────────────────────────────────────────────

/**
 * 每收到一条微信消息时调用。
 * 只处理一对一私聊（v1 跳过群消息）。
 * 对应 src/bridge.ts handleMessage() + enqueueMessage()。
 */
async function handleMessage(
  msg: WeixinMessage,
  sessionManager: SessionManager,
  config: WeChatAcpConfig,
) {
  // 只处理用户发来的消息（过滤掉系统通知、机器人自己的消息等）
  if (msg.message_type !== MessageType.USER) return;

  // v1 阶段跳过群消息，只处理私聊
  if (msg.group_id) return;

  const { from_user_id: userId, context_token: contextToken } = msg;

  // ── 入站格式转换 ──────────────────────────────────────────────────────────
  // 将微信消息（文本/图片/语音/文件/视频）转换为 ACP ContentBlock[]
  // 详见第六节 weixinMessageToPrompt()
  const prompt = await weixinMessageToPrompt(msg, config.wechat.cdnBaseUrl, console.log);

  // 将 prompt 投入该用户的串行消息队列
  // SessionManager 会确保同一用户的消息按顺序处理
  await sessionManager.enqueue(userId, { prompt, contextToken });
}

// ─────────────────────────────────────────────────────────────────────────────
// 二、多用户会话管理：SessionManager
// ─────────────────────────────────────────────────────────────────────────────

/**
 * 每个微信用户拥有独立的 Agent 子进程和消息队列。
 * SessionManager 负责：
 *   · 惰性创建会话（首次消息时 spawn Agent）
 *   · 串行化同一用户的消息处理（queue + processing 标志）
 *   · 容量控制：超出 maxConcurrentUsers 时驱逐最久未活跃的会话
 *   · 空闲清理：超过 idleTimeoutMs 未活动的会话自动销毁
 * 对应 src/acp/session.ts SessionManager。
 */
class SessionManager {
  // userId → UserSession 的映射表
  private sessions = new Map<string, UserSession>();
  private opts: SessionManagerOpts;

  start() {
    // 每 2 分钟检查一次空闲会话并清理
    setInterval(() => this.cleanupIdleSessions(), 2 * 60_000).unref();
  }

  async enqueue(userId: string, message: PendingMessage) {
    let session = this.sessions.get(userId);

    if (!session) {
      // 容量检查：超出上限时先驱逐最久未活跃的空闲会话
      if (this.sessions.size >= this.opts.maxConcurrentUsers) {
        this.evictOldest();
      }
      // 惰性创建：第一条消息时才 spawn Agent 子进程
      session = await this.createSession(userId, message.contextToken);
      this.sessions.set(userId, session);
    }

    // 更新最后活跃时间，入队
    session.contextToken = message.contextToken;
    session.lastActivity = Date.now();
    session.queue.push(message);

    // 若当前没有正在处理的消息，立即启动处理循环
    if (!session.processing) {
      session.processing = true;
      this.processQueue(session).catch(console.error);
    }
  }

  // ── 创建单个用户会话 ──────────────────────────────────────────────────────

  private async createSession(userId: string, contextToken: string): Promise<UserSession> {
    // 创建 ACP Client 实例（实现 Agent→Client 的回调接口）
    const client = new WeChatAcpClient({
      sendTyping: () => this.opts.sendTyping(userId, contextToken),
      log: (msg) => console.log(`[${userId}] ${msg}`),
    });

    // 启动 Agent 子进程并完成 ACP 握手（见第三节）
    const agentInfo = await spawnAgent({
      command: this.opts.agentCommand,
      args:    this.opts.agentArgs,
      cwd:     this.opts.agentCwd,
      env:     this.opts.agentEnv,
      client,
      log: (msg) => console.log(`[${userId}] ${msg}`),
    });

    // Agent 进程意外退出时，自动清理对应会话
    agentInfo.process.on("exit", () => {
      this.sessions.delete(userId);
    });

    return { userId, contextToken, client, agentInfo, queue: [], processing: false, lastActivity: Date.now(), createdAt: Date.now() };
  }

  // ── 消息处理循环 ──────────────────────────────────────────────────────────

  /**
   * 逐条处理队列中的消息，确保串行执行（同一用户不会并发调用 Agent）。
   * 对应 src/acp/session.ts processQueue()。
   */
  private async processQueue(session: UserSession) {
    try {
      while (session.queue.length > 0) {
        const pending = session.queue.shift()!;

        // 更新 typing 回调（让新消息的 contextToken 生效）
        session.client.updateSendTyping(() =>
          this.opts.sendTyping(session.userId, pending.contextToken),
        );

        // 清空上一轮遗留的 chunk 缓冲
        session.client.flush();

        // 向 Agent 发送 prompt，等待其完成（见第五节）
        const result = await session.agentInfo.connection.prompt({
          sessionId: session.agentInfo.sessionId,
          prompt:    pending.prompt,
        });

        // 取出 Agent 流式输出中累积的所有文本（见第四节 flush()）
        let replyText = session.client.flush();

        if (result.stopReason === "cancelled") replyText += "\n[cancelled]";
        if (result.stopReason === "refusal")   replyText += "\n[agent refused to continue]";

        // 通过回调把回复发回微信
        if (replyText.trim()) {
          await this.opts.onReply(session.userId, pending.contextToken, replyText);
        }
      }
    } finally {
      session.processing = false;
    }
  }

  // ── 空闲清理 & 驱逐 ───────────────────────────────────────────────────────

  /**
   * 每 2 分钟调用一次，杀死超过 idleTimeoutMs 未活动的会话。
   * 对应 src/acp/session.ts cleanupIdleSessions()。
   */
  private cleanupIdleSessions() {
    const now = Date.now();
    for (const [userId, session] of this.sessions) {
      const idleMs = now - session.lastActivity;
      if (idleMs > this.opts.idleTimeoutMs && !session.processing) {
        killAgent(session.agentInfo.process);
        this.sessions.delete(userId);
      }
    }
  }

  /**
   * 当会话数达到上限时，驱逐最久未活跃的空闲会话，为新用户腾出空间。
   * 对应 src/acp/session.ts evictOldest()。
   */
  private evictOldest() {
    let oldest: { userId: string; lastActivity: number } | null = null;
    for (const [userId, session] of this.sessions) {
      if (!session.processing && (!oldest || session.lastActivity < oldest.lastActivity)) {
        oldest = { userId, lastActivity: session.lastActivity };
      }
    }
    if (oldest) {
      const session = this.sessions.get(oldest.userId);
      if (session) killAgent(session.agentInfo.process);
      this.sessions.delete(oldest.userId);
    }
  }

  async stop() {
    for (const session of this.sessions.values()) {
      killAgent(session.agentInfo.process);
    }
    this.sessions.clear();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 三、启动 Agent 子进程 + ACP 握手
// ─────────────────────────────────────────────────────────────────────────────

/**
 * spawnAgent() 完成以下工作：
 *   1. 启动 Agent 子进程，stdin/stdout 作为 IPC 管道
 *   2. 将 Node.js Stream 包装为 Web Stream，交给 ACP SDK
 *   3. 通过 ndJsonStream 建立 NDJSON 双向帧通道
 *   4. 创建 ClientSideConnection（持有 ACP Client 实例）
 *   5. 调用 initialize() 完成协议握手
 *   6. 调用 newSession() 获取 sessionId
 * 对应 src/acp/agent-manager.ts spawnAgent()。
 */
async function spawnAgent(params: {
  command: string;
  args:    string[];
  cwd:     string;
  env?:    Record<string, string>;
  client:  WeChatAcpClient;
  log:     (msg: string) => void;
}): Promise<AgentProcessInfo> {
  const { command, args, cwd, env, client, log } = params;

  // ── 步骤 1：启动子进程 ────────────────────────────────────────────────────
  // stdio: ["pipe","pipe","inherit"]
  //   · stdin  (pipe)    → 客户端向 Agent 写 NDJSON 请求
  //   · stdout (pipe)    → 读取 Agent 的 NDJSON 响应
  //   · stderr (inherit) → Agent 的日志直接输出到父进程终端，方便调试
  const proc = spawn(command, args, {
    stdio: ["pipe", "pipe", "inherit"],
    cwd,
    env: { ...process.env, ...env },
  });

  // ── 步骤 2：将 Node.js Stream 桥接为 Web Stream ──────────────────────────
  // ACP SDK 使用 WHATWG Streams API，需要转换
  const input  = Writable.toWeb(proc.stdin!);                              // WritableStream
  const output = Readable.toWeb(proc.stdout!) as ReadableStream<Uint8Array>; // ReadableStream

  // ── 步骤 3：建立 NDJSON 双向帧通道 ──────────────────────────────────────
  // ndJsonStream 将原始字节流转换为"换行分隔的 JSON 对象"流
  // 每一条 ACP 消息都是一行 JSON，方便流式解析
  const stream = acp.ndJsonStream(input, output);

  // ── 步骤 4：创建 ACP 客户端连接 ─────────────────────────────────────────
  // ClientSideConnection 持有两样东西：
  //   · () => client   —— 工厂函数，ACP SDK 用来调用我们实现的回调方法
  //   · stream         —— NDJSON 双向通道
  const connection = new acp.ClientSideConnection(() => client, stream);

  // ── 步骤 5：ACP 握手 initialize() ────────────────────────────────────────
  // 向 Agent 声明：
  //   · 我们支持的协议版本
  //   · 客户端名称和版本（Agent 可能据此调整行为）
  //   · 客户端能力：我们实现了文件系统代理（fs.readTextFile / fs.writeTextFile）
  //     Agent 可以通过 ACP 协议请求我们代为读写文件
  log("Initializing ACP connection...");
  const initResult = await connection.initialize({
    protocolVersion: acp.PROTOCOL_VERSION,
    clientInfo: {
      name:    "wechat-acp",
      title:   "wechat-acp",
      version: "0.1.0",
    },
    clientCapabilities: {
      fs: {
        readTextFile:  true,  // 客户端可为 Agent 读文件
        writeTextFile: true,  // 客户端可为 Agent 写文件
      },
    },
  });
  log(`ACP initialized (protocol v${initResult.protocolVersion})`);

  // ── 步骤 6：创建 ACP 会话 newSession() ───────────────────────────────────
  // 一个 Agent 进程可以维护多个会话，但这里每个用户独占一个进程，
  // 所以每个进程只有一个 sessionId
  log("Creating ACP session...");
  const sessionResult = await connection.newSession({
    cwd,           // Agent 的工作目录，影响文件读写的相对路径
    mcpServers: [], // 可选：MCP 工具服务器列表（本项目未使用）
  });
  log(`ACP session created: ${sessionResult.sessionId}`);

  return { process: proc, connection, sessionId: sessionResult.sessionId };
}

/**
 * 优雅终止 Agent 进程：先发 SIGTERM，5 秒后若仍存活则 SIGKILL。
 * 对应 src/acp/agent-manager.ts killAgent()。
 */
function killAgent(proc: ChildProcess) {
  if (!proc.killed) {
    proc.kill("SIGTERM");
    setTimeout(() => {
      if (!proc.killed) proc.kill("SIGKILL");
    }, 5_000).unref();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 四、实现 ACP Client 接口：WeChatAcpClient
// ─────────────────────────────────────────────────────────────────────────────

/**
 * WeChatAcpClient 是"客户端侧"的回调接口实现。
 * ACP 协议是双向的：客户端不仅向 Agent 发请求，Agent 也会反向调用客户端的方法。
 * 我们需要实现 acp.Client 接口中的四个方法：
 *   · sessionUpdate()    —— Agent 推送流式输出 / 工具调用状态
 *   · requestPermission() —— Agent 请求执行某个操作前征得同意
 *   · readTextFile()     —— Agent 请求读取本地文件（文件系统代理）
 *   · writeTextFile()    —— Agent 请求写入本地文件（文件系统代理）
 * 对应 src/acp/client.ts WeChatAcpClient。
 */
class WeChatAcpClient implements acp.Client {
  // 累积 Agent 流式输出的文本片段，等 Agent 完成后一次性取走
  private chunks: string[] = [];
  // 上次发送 typing 状态的时间戳（节流用）
  private lastTypingAt = 0;
  private static readonly TYPING_INTERVAL_MS = 5_000; // typing 最高每 5 秒发一次

  constructor(private opts: { sendTyping: () => Promise<void>; log: (msg: string) => void }) {}

  // 允许 SessionManager 在每条新消息时更新 contextToken（typing API 需要它）
  updateSendTyping(sendTyping: () => Promise<void>) {
    this.opts = { ...this.opts, sendTyping };
  }

  // ── 4.1 sessionUpdate：Agent 流式推送 ────────────────────────────────────

  /**
   * Agent 每产出一段文本、每完成一次工具调用，都会通过此方法通知客户端。
   * 我们只关心四种事件类型：
   */
  async sessionUpdate(params: acp.SessionNotification) {
    const update = params.update;

    switch (update.sessionUpdate) {
      // Agent 流式输出文本片段
      // 每次只有几个字，需要累积到 chunks[] 中
      case "agent_message_chunk":
        if (update.content.type === "text") {
          this.chunks.push(update.content.text);
        }
        await this.maybeSendTyping();
        break;

      // Agent 开始调用某个工具（如执行命令、读写文件等）
      case "tool_call":
        this.opts.log(`[tool] ${update.title} (${update.status})`);
        await this.maybeSendTyping();
        break;

      // 工具调用完成，可能携带 diff（文件修改内容）
      // 我们把 diff 格式化成 markdown 代码块，追加到 chunks 中，
      // 这样用户能在微信中看到 Agent 改了哪些代码
      case "tool_call_update":
        if (update.status === "completed" && update.content) {
          for (const c of update.content) {
            if (c.type === "diff") {
              const diff = c as acp.Diff;
              const lines: string[] = [`--- ${diff.path}`];
              if (diff.oldText) for (const l of diff.oldText.split("\n")) lines.push(`- ${l}`);
              if (diff.newText) for (const l of diff.newText.split("\n")) lines.push(`+ ${l}`);
              this.chunks.push("\n```diff\n" + lines.join("\n") + "\n```\n");
            }
          }
        }
        break;

      // Agent 规划了执行步骤，逐条记录日志（不发给用户）
      case "plan":
        if (update.entries) {
          const items = update.entries
            .map((e: acp.PlanEntry, i: number) => `  ${i + 1}. [${e.status}] ${e.content}`)
            .join("\n");
          this.opts.log(`[plan]\n${items}`);
        }
        break;
    }
  }

  // ── 4.2 requestPermission：自动批准所有权限请求 ──────────────────────────

  /**
   * Agent 在执行敏感操作前（如运行命令、修改文件）会调用此方法请求授权。
   * 这里实现了"yolo 模式"：找到第一个 allow_once 或 allow_always 选项，自动批准。
   * ⚠️ 安全提示：这意味着 Agent 的任何操作请求都会被允许，
   *    生产环境应根据需要实现更精细的权限控制。
   */
  async requestPermission(params: acp.RequestPermissionRequest): Promise<acp.RequestPermissionResponse> {
    // 优先选择 allow_once 或 allow_always
    const allowOpt = params.options.find(
      (o) => o.kind === "allow_once" || o.kind === "allow_always",
    );
    const optionId = allowOpt?.optionId ?? params.options[0]?.optionId ?? "allow";

    this.opts.log(`[permission] auto-allowed: ${params.toolCall?.title ?? "unknown"} → ${optionId}`);

    return { outcome: { outcome: "selected", optionId } };
  }

  // ── 4.3 文件系统代理：readTextFile / writeTextFile ──────────────────────

  /**
   * Agent 通过 ACP 协议请求读取文件时，由我们代为执行 fs.readFile。
   * 这让 Agent 能访问项目代码文件，即使它运行在沙箱中。
   */
  async readTextFile(params: acp.ReadTextFileRequest): Promise<acp.ReadTextFileResponse> {
    const content = await fs.promises.readFile(params.path, "utf-8");
    return { content };
  }

  /**
   * Agent 通过 ACP 协议请求写入文件时，由我们代为执行 fs.writeFile。
   * Agent 改完代码后通过这个接口把新内容写回磁盘。
   */
  async writeTextFile(params: acp.WriteTextFileRequest): Promise<acp.WriteTextFileResponse> {
    await fs.promises.writeFile(params.path, params.content, "utf-8");
    return {};
  }

  // ── 4.4 flush()：取出累积的回复文本 ──────────────────────────────────────

  /**
   * Agent 完成一轮对话后，调用 flush() 取出所有累积的 chunk，
   * 拼接成完整的回复字符串，并清空缓冲区，为下一轮做准备。
   *
   * 流式输出示意：
   *   sessionUpdate("Hel") → sessionUpdate("lo, ") → sessionUpdate("world")
   *   flush() → "Hello, world"   (清空 chunks[])
   */
  flush(): string {
    const text = this.chunks.join("");
    this.chunks = [];
    this.lastTypingAt = 0;
    return text;
  }

  // typing 节流：避免对微信 API 过度请求（每轮对话最多每 5 秒发一次）
  private async maybeSendTyping() {
    const now = Date.now();
    if (now - this.lastTypingAt < WeChatAcpClient.TYPING_INTERVAL_MS) return;
    this.lastTypingAt = now;
    try { await this.opts.sendTyping(); } catch { /* best-effort */ }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 五、发送 prompt 并收集回复（已内嵌在 processQueue 中，这里单独展示）
// ─────────────────────────────────────────────────────────────────────────────

/**
 * 向 Agent 发送一轮对话，等待其完成并收集回复。
 * 这是整个链路的"心跳"——每条用户消息最终都走这里。
 * 对应 src/acp/session.ts processQueue() 的核心部分。
 */
async function sendPromptAndCollect(
  connection: acp.ClientSideConnection,
  sessionId:  string,
  client:     WeChatAcpClient,
  prompt:     acp.ContentBlock[],
): Promise<string> {
  // 清空上一轮遗留的 chunk，避免串话
  client.flush();

  // connection.prompt() 是异步阻塞的：
  //   · 内部通过 NDJSON 流向 Agent 发送请求
  //   · Agent 流式输出 → 触发 client.sessionUpdate() → chunk 被累积
  //   · 当 Agent 发出"完成"信号（stop），此 Promise resolve
  const result = await connection.prompt({
    sessionId,
    prompt,  // acp.ContentBlock[]，可包含文本/图片/文件资源
  });

  // 此时 WeChatAcpClient.chunks 中已积累了所有流式 chunk
  // flush() 拼接并清空，返回完整回复文本
  let replyText = client.flush();

  // 处理特殊终止原因
  if (result.stopReason === "cancelled") replyText += "\n[cancelled]";
  if (result.stopReason === "refusal")   replyText += "\n[agent refused to continue]";

  return replyText;
}

// ─────────────────────────────────────────────────────────────────────────────
// 六、消息格式转换：入站（微信 → ACP）& 出站（ACP → 微信）
// ─────────────────────────────────────────────────────────────────────────────

// ── 6.1 入站：weixinMessageToPrompt() ────────────────────────────────────

/**
 * 将一条微信消息转换为 ACP ContentBlock[]。
 * ACP 支持多模态内容：文本、图片（base64）、文件资源（URI + text）。
 * 这里把微信的各种消息类型映射到对应的 ContentBlock。
 * 对应 src/adapter/inbound.ts weixinMessageToPrompt()。
 *
 * 转换规则：
 *   文本消息    → { type: "text", text }
 *   语音消息    → { type: "text", text: transcription }  (有转写时)
 *   图片消息    → { type: "image", data: base64, mimeType: "image/jpeg" }
 *   文本文件    → { type: "resource", resource: { uri, mimeType, text } }
 *   二进制文件  → { type: "text", text: "[Received file: xxx, N bytes]" }
 *   视频消息    → { type: "text", text: "[Received video message]" }
 */
async function weixinMessageToPrompt(
  msg:        WeixinMessage,
  cdnBaseUrl: string,
  log:        (msg: string) => void,
): Promise<acp.ContentBlock[]> {
  const blocks: acp.ContentBlock[] = [];

  // 提取文本（含引用消息的上下文）
  const text = extractText(msg.item_list);
  if (text) blocks.push({ type: "text", text });

  // 尝试下载并转换媒体附件
  // 媒体文件存储在微信 CDN，需要用 AES 密钥解密
  const mediaItem = findMediaItem(msg.item_list);
  if (mediaItem) {
    try {
      const mediaBlock = await convertMediaItem(mediaItem, cdnBaseUrl, log);
      if (mediaBlock) blocks.push(mediaBlock);
    } catch (err) {
      log(`Media download failed, skipping: ${String(err)}`);
      blocks.push({ type: "text", text: "[Received media - download failed]" });
    }
  }

  // 保证至少有一个 block（ACP prompt 不能为空）
  if (blocks.length === 0) blocks.push({ type: "text", text: "[empty message]" });

  return blocks;
}

/**
 * 从 item_list 中提取文本内容。
 * 支持普通文本、引用消息（附加引用上下文）、语音转文字。
 */
function extractText(itemList?: MessageItem[]): string {
  for (const item of itemList ?? []) {
    if (item.type === MessageItemType.TEXT && item.text_item?.text != null) {
      const text = String(item.text_item.text);
      // 如果用户引用了某条消息，拼上引用内容以提供上下文
      const ref = item.ref_msg;
      if (!ref) return text;
      const parts = [ref.title, ref.message_item?.text_item?.text].filter(Boolean);
      return parts.length ? `[引用: ${parts.join(" | ")}]\n${text}` : text;
    }
    // 语音消息有 ASR 转写时，直接用转写文本
    if (item.type === MessageItemType.VOICE && item.voice_item?.text) {
      return item.voice_item.text;
    }
  }
  return "";
}

/**
 * 下载并解密 CDN 媒体文件，转换为 ACP ContentBlock。
 * 微信 CDN 的媒体文件通过 AES 加密存储，需要从消息中解析密钥再解密。
 */
async function convertMediaItem(
  item:       MessageItem,
  cdnBaseUrl: string,
  log:        (msg: string) => void,
): Promise<acp.ContentBlock | null> {
  if (item.type === MessageItemType.IMAGE && item.image_item?.media) {
    log("Downloading image from CDN...");
    const aesKey = parseAesKey(item.image_item.media);
    const buffer = await downloadAndDecrypt(item.image_item.media.encrypt_query_param!, aesKey, cdnBaseUrl);
    // 图片以 base64 编码传给 Agent（ACP image block）
    return { type: "image", data: buffer.toString("base64"), mimeType: "image/jpeg" } as acp.ContentBlock;
  }

  if (item.type === MessageItemType.FILE && item.file_item?.media) {
    log(`Downloading file "${item.file_item.file_name}" from CDN...`);
    const aesKey = parseAesKey(item.file_item.media);
    const buffer = await downloadAndDecrypt(item.file_item.media.encrypt_query_param!, aesKey, cdnBaseUrl);
    const fileName = item.file_item.file_name ?? "file";

    // 文本类文件（.ts/.py/.md 等）以 resource block 形式传入，Agent 能直接阅读内容
    if (isTextFile(fileName)) {
      return {
        type: "resource",
        resource: { uri: `file:///${fileName}`, mimeType: guessMimeType(fileName), text: buffer.toString("utf-8") },
      } as acp.ContentBlock;
    }
    // 二进制文件无法嵌入，描述一下让 Agent 知道有此附件
    return { type: "text", text: `[Received file: ${fileName}, ${buffer.length} bytes]` };
  }

  if (item.type === MessageItemType.VIDEO) {
    return { type: "text", text: "[Received video message]" };
  }

  return null;
}

// ── 6.2 出站：formatForWeChat() ──────────────────────────────────────────

/**
 * Agent 的回复通常包含 Markdown 标记（**粗体**、# 标题、[链接](url) 等）。
 * 微信文本消息不渲染 Markdown，直接显示会充满星号和井号，体验很差。
 * 这里做简单的剥离处理，保留可读性，同时保留代码块（``` ... ```）。
 * 对应 src/adapter/outbound.ts formatForWeChat()。
 */
function formatForWeChat(text: string): string {
  let out = text;
  // 图片引用 ![alt](url) → [alt]（去掉 URL，保留描述）
  out = out.replace(/!\[([^\]]*)\]\([^)]+\)/g, "[$1]");
  // 链接 [text](url) → text (url)（展开 URL，微信能识别纯文本 URL）
  out = out.replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1 ($2)");
  // 粗斜体 ***text*** / **text** / *text* / __text__ / _text_ → text
  out = out.replace(/\*\*\*(.+?)\*\*\*/g, "$1");
  out = out.replace(/\*\*(.+?)\*\*/g,     "$1");
  out = out.replace(/\*(.+?)\*/g,         "$1");
  out = out.replace(/__(.+?)__/g,         "$1");
  out = out.replace(/_(.+?)_/g,           "$1");
  // 标题 ## 标题文本 → 标题文本
  out = out.replace(/^#{1,6}\s+/gm, "");
  // 压缩多余空行
  out = out.replace(/\n{3,}/g, "\n\n");
  return out.trim();
}

// ─────────────────────────────────────────────────────────────────────────────
// 附录：内置 Agent 预设（src/config.ts BUILT_IN_AGENTS）
// ─────────────────────────────────────────────────────────────────────────────

/**
 * 项目内置了 6 种主流 AI 编码代理的快捷启动配置。
 * 用户通过 --agent copilot / --agent claude 等参数选用。
 * 每种 Agent 都遵循 ACP 协议，wechat-acp 客户端侧无需为不同 Agent 做特殊适配。
 */
const BUILT_IN_AGENTS = {
  copilot:  { command: "npx", args: ["@github/copilot",             "--acp", "--yolo"]              },
  claude:   { command: "npx", args: ["@zed-industries/claude-code-acp"]                              },
  gemini:   { command: "npx", args: ["@google/gemini-cli",          "--experimental-acp"]            },
  qwen:     { command: "npx", args: ["@qwen-code/qwen-code",        "--acp", "--experimental-skills"] },
  codex:    { command: "npx", args: ["@zed-industries/codex-acp"]                                    },
  opencode: { command: "npx", args: ["opencode-ai",                 "acp"]                           },
};
```
