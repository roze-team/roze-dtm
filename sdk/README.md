# Roze DTM Web SDK

`roze-dtm.ts` 和 `roze-dtm.js` 覆盖原生 `/v1` 控制面的五种事务提交、生命周期转换、查询、恢复、统计、Dashboard 与 XA 对账接口。客户端接受可选 Bearer token、`AbortSignal`、附加请求头和自定义 Fetch 实现，并按 Roze 数字响应合同解包 `data`；HTTP 失败或非零业务码抛出 `RozeDtmApiError`。

```ts
import { RozeDtmClient } from "./roze-dtm.js";

const dtm = new RozeDtmClient("https://dtm.example.com", process.env.DTM_TOKEN);
const transaction = await dtm.submitSaga({
  gid: "order-20260828",
  branches: [{
    id: "reserve",
    action: "https://inventory.example.com/reserve",
    compensate: "https://inventory.example.com/release",
    payload: { sku: "A-1", quantity: 1 },
  }],
});
```

当前 Roze 1.0 `.api` 标量集合尚不能无损表达递归自由 JSON；因此这里保留真实的 `JsonValue` 类型，没有把分支 `payload` 错误收窄为字符串或固定对象。待生成器提供自由 JSON 类型后，再以 `.api` 生成 OpenAPI 和 Web SDK，并用合同差异检查替换此过渡实现。

## dtm-labs 兼容客户端

`roze-dtm-compat.ts` 和 `roze-dtm-compat.js` 覆盖 `/api/dtmsvr/**` 的版本、GID、事务操作、动态分支、callback Workflow、查询分页、恢复管理、topic/KV，以及 `/api/json-rpc` 的五个方法。`version()` 同时返回包版本和可空的部署 Git revision。客户端按上游原始 `dtm_result` 或 JSON-RPC 2.0 合同处理响应，不会将其误当作 Roze envelope。

```ts
import { RozeDtmCompatClient } from "./roze-dtm-compat.js";

const compat = new RozeDtmCompatClient("https://dtm.example.com", process.env.DTM_TOKEN);
const { gid } = await compat.newGid();
await compat.prepare({ gid, trans_type: "tcc" });
```
