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
    // 初始化 Neon Serverless 驱动
    const sql = neon(env.DATABASE_URL);
    const reqId = crypto.randomUUID();

    try {
      // 1. 解析请求路径和查询参数
      const url = new URL(request.url);
      const pathWithQuery = url.pathname + url.search;

      // 2. 解析 Headers
      const headers: Record<string, string> = {};
      request.headers.forEach((value, key) => {
        if (!key.toLowerCase().startsWith('cf-') && key.toLowerCase() !== 'host') {
          headers[key] = value;
        }
      });

      // 3. 解析并 Base64 编码 Body
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

      // 4. 将任务写入 PostgreSQL 并触发 NOTIFY 通知 Go 客户端
      // 注意：这里必须拆分为两次独立的查询，不能用分号连在一起
      const reqJsonStr = JSON.stringify(proxyReq);
      
      await sql`
        INSERT INTO tunnel_tasks (req_id, req_data) 
        VALUES (${reqId}, ${reqJsonStr}::jsonb)
      `;

      await sql`
        SELECT pg_notify('tunnel_channel', ${reqId})
      `;

      // 5. 异步轮询等待 Go 客户端返回的数据库结果
      const startTime = Date.now();
      const timeoutMs = 15000; // 15秒超时
      const pollIntervalMs = 50; // 每隔 50ms 轮询一次

      while (Date.now() - startTime < timeoutMs) {
        // HTTP API 模式查询，极度轻量且不占用长连接
        const rows = await sql`
          SELECT res_data FROM tunnel_tasks 
          WHERE req_id = ${reqId} AND status = 'done'
        `;
        
        if (rows.length > 0 && rows[0].res_data) {
          const proxyRes = rows[0].res_data;
          
          // 组装最终响应的 Headers
          const responseHeaders = new Headers();
          for (const [key, value] of Object.entries(proxyRes.headers || {})) {
            responseHeaders.set(key, value as string);
          }

          // Base64 解码响应体
          let responseBody: Uint8Array | null = null;
          if (proxyRes.body) {
            responseBody = base64ToUint8Array(proxyRes.body);
          }

          // 异步清理已完成的任务，保持数据库干净 (waitUntil 允许在返回后继续执行)
          ctx.waitUntil(sql`DELETE FROM tunnel_tasks WHERE req_id = ${reqId}`);

          // 返回标准 Response 给访客
          return new Response(responseBody, {
            status: proxyRes.status_code,
            headers: responseHeaders
          });
        }
        
        // 延时后继续下一次轮询
        await new Promise(resolve => setTimeout(resolve, pollIntervalMs));
      }

      // 6. 超时处理及清理僵尸记录
      ctx.waitUntil(sql`DELETE FROM tunnel_tasks WHERE req_id = ${reqId}`);
      return new Response("Gateway Timeout: 本地服务未响应", { status: 504 });

    } catch (err: any) {
      // 发生异常时也尝试清理记录
      ctx.waitUntil(sql`DELETE FROM tunnel_tasks WHERE req_id = ${reqId}`);
      return new Response(`Worker Error: ${err.message}`, { status: 500 });
    }
  },
};
