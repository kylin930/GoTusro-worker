import { neon } from '@neondatabase/serverless';

export interface Env {
  DATABASE_URL: string;
}

// 辅助函数：将 ArrayBuffer 转换为 Base64 字符串
function arrayBufferToBase64(buffer: ArrayBuffer): string {
  let binary = '';
  const bytes = new Uint8Array(buffer);
  for (let i = 0; i < bytes.byteLength; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}

// 辅助函数：将 Base64 字符串转换为 Uint8Array
function base64ToUint8Array(base64: string): Uint8Array {
  const binaryString = atob(base64);
  const bytes = new Uint8Array(binaryString.length);
  for (let i = 0; i < binaryString.length; i++) {
    bytes[i] = binaryString.charCodeAt(i);
  }
  return bytes;
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const workerStartTime = Date.now(); // 记录 Worker 介入的第一刻
    let reqId = "init";
    let sql: any;

    try {
      // 1. 严格检查环境变量
      if (!env.DATABASE_URL) {
        return new Response("配置致命错误: 环境变量 DATABASE_URL 未设置！请检查 wrangler.toml", { status: 500, headers: {"Content-Type": "text/plain; charset=utf-8"} });
      }

      sql = neon(env.DATABASE_URL);
      reqId = crypto.randomUUID();

      const url = new URL(request.url);
      
      // === 调试模式拦截 ===
      const isDebug = url.pathname === '/_gotusro/debug';
      let pathWithQuery = url.pathname + url.search;
      
      // 如果是调试请求，将其重定向到本地的根路径进行测试
      if (isDebug) {
          pathWithQuery = '/';
      }

      const headers: Record<string, string> = {};
      request.headers.forEach((value, key) => {
        if (!key.toLowerCase().startsWith('cf-') && key.toLowerCase() !== 'host') {
          headers[key] = value;
        }
      });

      let bodyBase64 = "";
      if (request.body && !isDebug) { // 调试时忽略 body
        const arrayBuffer = await request.arrayBuffer();
        if (arrayBuffer.byteLength > 0) {
          bodyBase64 = arrayBufferToBase64(arrayBuffer);
        }
      }

      // 2. 组装请求，打上时间戳
      const proxyReq = {
        req_id: reqId,
        hostname: url.hostname,
        method: isDebug ? "GET" : request.method,
        path: pathWithQuery,
        headers: headers,
        body: bodyBase64,
        worker_send_time: Date.now() // 新增：发往数据库前的时间戳
      };

      // 3. 写入任务到数据库 (已拆分命令防止注入保护报错)
      const reqJsonStr = JSON.stringify(proxyReq);
      await sql`
        INSERT INTO tunnel_tasks (req_id, req_data) 
        VALUES (${reqId}, ${reqJsonStr}::jsonb)
      `;

      // 记录开始等待数据库响应的时间
      const dbWaitStartTime = Date.now();
      await sql`SELECT pg_notify('tunnel_channel', ${reqId})`;

      // 4. 轮询等待 Go 客户端结果
      const timeoutMs = 15000; 
      const pollIntervalMs = 50; 

      while (Date.now() - dbWaitStartTime < timeoutMs) {
        const rows = await sql`
          SELECT res_data FROM tunnel_tasks 
          WHERE req_id = ${reqId} AND status = 'done'
        `;
        
        if (rows.length > 0 && rows[0].res_data) {
          const proxyRes = rows[0].res_data;
          
          // 优雅清理：添加 .catch 防止清理过程报错引发异常
          ctx.waitUntil(sql`DELETE FROM tunnel_tasks WHERE req_id = ${reqId}`.catch(()=>{}));

          // === 如果是调试模式，返回纯文本探针数据 ===
          if (isDebug) {
             const workerTotalTime = Date.now() - workerStartTime;
             const dbWaitTime = Date.now() - dbWaitStartTime;
             const receiveMs = proxyRes.receive_ms || 0;
             const forwardMs = proxyRes.forward_ms || 0;
             const renderMs = proxyRes.render_ms || 0;

             const debugText = 
`=============================
GoTusro 链路探针
=============================
[1] Worker 节点总耗时: ${workerTotalTime} ms
[2] 数据库等待与轮询耗时: ${dbWaitTime} ms

--- Go 客户端内部拆解 ---
[3] 接收延迟 (Worker发往DB -> Go读取完毕): ${receiveMs} ms
[4] 发送耗时 (Go请求本地服务 -> 拿到结果): ${forwardMs} ms
[5] 渲染耗时 (DB读取/Base64/JSON解析等): ${renderMs} ms

* 提示: 接收延迟(3)依赖 Worker 与本地服务器时钟同步。若本地未开 NTP 对时，此项可能不准。`;

             return new Response(debugText, {
                 status: 200,
                 headers: { "Content-Type": "text/plain; charset=utf-8" }
             });
          }

          // 正常模式，返回真实响应
          const responseHeaders = new Headers();
          for (const [key, value] of Object.entries(proxyRes.headers || {})) {
            responseHeaders.set(key, value as string);
          }

          let responseBody: Uint8Array | null = null;
          if (proxyRes.body) {
            responseBody = base64ToUint8Array(proxyRes.body);
          }

          return new Response(responseBody, {
            status: proxyRes.status_code,
            headers: responseHeaders
          });
        }
        
        await new Promise(resolve => setTimeout(resolve, pollIntervalMs));
      }

      // 超时清理
      ctx.waitUntil(sql`DELETE FROM tunnel_tasks WHERE req_id = ${reqId}`.catch(()=>{}));
      return new Response("Gateway Timeout: 本地 Go 服务未响应（请检查本地终端）", { status: 504, headers: {"Content-Type": "text/plain; charset=utf-8"} });

    } catch (err: any) {
      // 5. 终极兜底：拦截任何可能的报错，打印排查信息
      try {
         if (sql && reqId !== "init") {
            ctx.waitUntil(sql`DELETE FROM tunnel_tasks WHERE req_id = ${reqId}`.catch(()=>{}));
         }
      } catch(e) {}
      
      return new Response(`Worker内部故障\n\n【错误详情】: ${err.message}\n\n【调用栈】:\n${err.stack}`, { 
         status: 500,
         headers: { "Content-Type": "text/plain; charset=utf-8" }
      });
    }
  },
};
