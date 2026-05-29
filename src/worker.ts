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
      const pathWithQuery = url.pathname + url.search;

      const headers: Record<string, string> = {};
      request.headers.forEach((value, key) => {
        if (!key.toLowerCase().startsWith('cf-') && key.toLowerCase() !== 'host') {
          headers[key] = value;
        }
      });

      let bodyBase64 = "";
      if (request.body) {
        const arrayBuffer = await request.arrayBuffer();
        if (arrayBuffer.byteLength > 0) {
          bodyBase64 = arrayBufferToBase64(arrayBuffer);
        }
      }

      const proxyReq = {
        req_id: reqId,
        hostname: url.hostname,
        method: request.method,
        path: pathWithQuery,
        headers: headers,
        body: bodyBase64
      };

      // 2. 写入任务到数据库 (已拆分命令)
      const reqJsonStr = JSON.stringify(proxyReq);
      await sql`
        INSERT INTO tunnel_tasks (req_id, req_data) 
        VALUES (${reqId}, ${reqJsonStr}::jsonb)
      `;

      await sql`
        SELECT pg_notify('tunnel_channel', ${reqId})
      `;

      // 3. 轮询等待 Go 客户端结果
      const startTime = Date.now();
      const timeoutMs = 15000; 
      const pollIntervalMs = 50; 

      while (Date.now() - startTime < timeoutMs) {
        const rows = await sql`
          SELECT res_data FROM tunnel_tasks 
          WHERE req_id = ${reqId} AND status = 'done'
        `;
        
        if (rows.length > 0 && rows[0].res_data) {
          const proxyRes = rows[0].res_data;
          
          const responseHeaders = new Headers();
          for (const [key, value] of Object.entries(proxyRes.headers || {})) {
            responseHeaders.set(key, value as string);
          }

          let responseBody: Uint8Array | null = null;
          if (proxyRes.body) {
            responseBody = base64ToUint8Array(proxyRes.body);
          }

          // 优雅清理：添加 .catch 防止清理过程报错引发异常
          ctx.waitUntil(sql`DELETE FROM tunnel_tasks WHERE req_id = ${reqId}`.catch(()=>{}));

          return new Response(responseBody, {
            status: proxyRes.status_code,
            headers: responseHeaders
          });
        }
        
        await new Promise(resolve => setTimeout(resolve, pollIntervalMs));
      }

      ctx.waitUntil(sql`DELETE FROM tunnel_tasks WHERE req_id = ${reqId}`.catch(()=>{}));
      return new Response("Gateway Timeout: 本地 Go 服务未响应（请检查本地终端）", { status: 504, headers: {"Content-Type": "text/plain; charset=utf-8"} });

    } catch (err: any) {
      // 4. 终极兜底：拦截任何可能的报错，直接把错误明细打印到浏览器上
      try {
         if (sql && reqId !== "init") {
            ctx.waitUntil(sql`DELETE FROM tunnel_tasks WHERE req_id = ${reqId}`.catch(()=>{}));
         }
      } catch(e) {}
      
      // 这里的排版能让你一眼看清到底是哪里炸了
      return new Response(`Worker 内部故障排查\n\n【错误详情】: ${err.message}\n\n【调用栈】:\n${err.stack}`, { 
         status: 500,
         headers: { "Content-Type": "text/plain; charset=utf-8" }
      });
    }
  },
};
