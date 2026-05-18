// worker.ts

export interface Env {
  UPSTASH_REDIS_REST_URL: string;
  UPSTASH_REDIS_REST_TOKEN: string;
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
    try {
      // 1. 生成唯一请求 ID
      const reqId = crypto.randomUUID();

      // 2. 解析请求路径和查询参数
      const url = new URL(request.url);
      const pathWithQuery = url.pathname + url.search;

      // 解析 Headers
      const headers: Record<string, string> = {};
      request.headers.forEach((value, key) => {
        // 过滤掉一些可能影响本地转发的 Cloudflare 特有请求头
        if (!key.toLowerCase().startsWith('cf-') && key.toLowerCase() !== 'host') {
          headers[key] = value;
        }
      });

      // 解析并 Base64 编码 Body
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

      // 3. 将任务推送到 Upstash Redis 队列 (LPUSH)
      const pushUrl = `${env.UPSTASH_REDIS_REST_URL}/lpush/tunnel:requests`;
      await fetch(pushUrl, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${env.UPSTASH_REDIS_REST_TOKEN}` },
        body: JSON.stringify(proxyReq)
      });

      // 4. 异步轮询等待 Go 客户端的结果
      const startTime = Date.now();
      const timeoutMs = 15000; // 15秒超时
      const pollIntervalMs = 50; // 每隔 50ms 轮询一次
      const getUrl = `${env.UPSTASH_REDIS_REST_URL}/get/tunnel:response:${reqId}`;

      while (Date.now() - startTime < timeoutMs) {
        const res = await fetch(getUrl, {
          headers: { 'Authorization': `Bearer ${env.UPSTASH_REDIS_REST_TOKEN}` }
        });
        
        if (res.ok) {
          const data = await res.json() as any;
          
          // Upstash REST API 返回格式为 {"result": "值"}，如果键不存在，result 为 null
          if (data.result !== null) {
            // 解析 Go 传回的 JSON 数据
            const proxyRes = JSON.parse(data.result);
            
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

            // 返回标准 Response 给访客
            return new Response(responseBody, {
              status: proxyRes.status_code,
              headers: responseHeaders
            });
          }
        }
        
        // 延时后继续下一次轮询
        await new Promise(resolve => setTimeout(resolve, pollIntervalMs));
      }

      // 5. 超时处理
      return new Response("Gateway Timeout: 本地服务未响应", { status: 504 });

    } catch (err: any) {
      return new Response(`Worker Error: ${err.message}`, { status: 500 });
    }
  },
};