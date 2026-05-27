# ray-pixi: `runtime_env={"pixi": ...}` 插件设计

- 日期: 2026-05-27
- 状态: 已批准设计,待实现

## 目标

为 Ray 提供一个 pixi 版的 runtime environment,类比 Ray 内置的 `uv` 字段
(`ray._private.runtime_env.uv.UvPlugin`)。用户可在 `runtime_env` 中声明一个
pixi 环境,Ray 在每个节点的 runtime_env_agent 上把该环境落地并安装,然后让
worker 在该环境内启动。

采用**服务端插件方式**(对应 `runtime_env={"uv": [...]}`),而非 `uv run` 那种
driver 端 hook 方式。

## 非目标

- 不实现 `pixi run` 自动探测的 driver 端 hook(对应 uv 的
  `uv_runtime_env_hook.py`)。可作为后续扩展。
- 不自动向 pixi 环境注入与集群匹配的 ray 版本。由用户清单自行声明兼容的 ray。

## 背景:Ray runtime_env 插件机制

研究 `.venv/.../ray/_private/runtime_env/` 后确认:

- 插件继承 `ray._private.runtime_env.plugin.RuntimeEnvPlugin`(`@DeveloperAPI`),
  设置类属性 `name`(即 runtime_env 字段键,这里为 `"pixi"`),实现
  `validate()`、`get_uris()`、`create()`、`modify_context()`、`delete_uri()`。
- 第三方插件通过环境变量 `RAY_RUNTIME_ENV_PLUGINS` 注册,值为 JSON 列表,如
  `[{"class": "ray_pixi.PixiPlugin", "priority": 10}]`。该变量必须在**每个节点的
  runtime_env_agent 进程**上可见(即 `ray start` / `ray.init()` 之前设置)。
- `RuntimeEnvPluginSchemaManager.validate(name, value)` 在 driver 端 `RuntimeEnv`
  赋值时调用;若未注册对应 JSON schema 则跳过校验,因此 `pixi` 字段可自由通过。
- 插件字段**没有** driver 端 transform 钩子;插件的 `validate()` 静态方法在
  **agent 端**(安装时)运行。这一约束直接决定了第 5 节「清单如何到达 worker」。
- 缓存模式参考 `UvPlugin`:per-URI `asyncio.Lock`、`_created_hash_bytes`、
  `_creating_task`(delete 时取消正在进行的安装)。
- worker 启动通过 `RuntimeEnvContext.py_executable`(以及可选的 `command_prefix`)
  改写;`uv` hook 即把 `py_executable` 设为 `uv run ... python`。

## 架构

### 模块布局

```
src/ray_pixi/
  __init__.py      # 公开 API:PixiPlugin、pixi()、plugin_config()、__version__
  _plugin.py       # PixiPlugin(RuntimeEnvPlugin 子类,name="pixi")
  _processor.py    # PixiProcessor:执行环境落地与安装(对应 UvProcessor)
  _manifest.py     # 清单落地:path 内容写入 / dict 合成 pixi.toml
  _binary.py       # pixi 可执行文件解析:PATH 查找 + 按 pixi_version 引导下载
  _spec.py         # pixi 字段解析/校验 + URI 哈希
```

每个文件单一职责,可独立测试。`__init__.py` 仅做 re-export,保持公开面收敛。

### 公开 API

```python
from ray_pixi import PixiPlugin, pixi, plugin_config

# 推荐入口:driver 端读取清单 + lock 并内联,生成自包含 spec
ray.init(runtime_env={"pixi": pixi("pixi.toml", environment="default", locked=True)})

# 注册配置(用于设置 RAY_RUNTIME_ENV_PLUGINS)
plugin_config()  # -> {"class": "ray_pixi.PixiPlugin"}
```

## 字段形态(已确认 schema)

`pixi` 字段接受 `str` 或 `dict`:

- `str`:等价于 `{"manifest": <str>}`(对应 conda 接受路径字符串)。
- `dict`,**清单来源二选一**:
  - `manifest`: 指向 `pixi.toml` / `pyproject.toml` 的路径;或由 `pixi()` 内联后
    携带 `manifest_content`(+ `lock_content`、`manifest_format`)。
  - 内联 spec:`channels` (List[str])、`dependencies` (Dict[str, str], conda 包)、
    `pypi_dependencies` (Dict[str, str])、`platforms` (List[str])。
- 通用键:
  - `environment` (str, 默认 `"default"`):选 manifest 中哪个环境(`pixi -e`)。
  - `locked` (bool, 默认 `False`):严格按 `pixi.lock` 复现(`--locked`/`--frozen`)。
  - `pixi_version` (str, 可选):指定则自动引导下载该版本 pixi,否则用 PATH 上的。
  - `pixi_install_options` (List[str], 默认 `[]`):透传给 `pixi install` 的额外 flag。

**校验规则**(`validate()` / `_spec.py`):`manifest`(或内联内容)与内联 spec 键
互斥,且必须恰好提供其一;否则抛 `ValueError`。

### `pixi()` 辅助函数

签名(草案):

```python
def pixi(
    manifest: str | dict | None = None,
    *,
    channels: list[str] | None = None,
    dependencies: dict[str, str] | None = None,
    pypi_dependencies: dict[str, str] | None = None,
    platforms: list[str] | None = None,
    environment: str = "default",
    locked: bool = False,
    pixi_version: str | None = None,
    pixi_install_options: list[str] | None = None,
) -> dict:
    ...
```

- 当 `manifest` 是路径:在 driver 端读取其内容,若同目录存在 `pixi.lock` 也一并
  读取,返回内联了 `manifest_content` / `lock_content` / `manifest_format` 的 dict。
- 当传内联 spec(channels 等):原样规范化返回。
- 返回的 dict 自包含、跨节点安全,可直接作为 `runtime_env["pixi"]`。

## URI 与缓存

- `get_uri(runtime_env) -> "pixi://<sha1>" | None`。
- sha1 基于**规范化后的 spec 内容**:清单内容 + lock 内容 + `environment` +
  `locked` + `pixi_version`。清单**内容**(而非路径)参与哈希:相同清单跨任务
  共享缓存,清单/lock 改动自动失效。
- 路径模式但未内联内容(见第「清单如何到达 worker」)时,哈希基于可获得的
  规范化输入;实现需保证确定性。

## `create()` 流程(PixiProcessor)

1. `target_dir = <resources_dir>/pixi/<hash>/`,创建。
2. 解析 pixi 可执行(`_binary.py`):
   - 设了 `pixi_version` → 引导下载该版本到 `target_dir/.pixi-bin`。
   - 否则用 PATH 上的 `pixi`;缺失则抛 `RuntimeError`,提示安装或指定版本。
3. 落地清单到 `target_dir`(`_manifest.py`):
   - 内联内容(`manifest_content`)→ 直接写为 `pixi.toml`(或对应格式)。
   - dict spec → 用 `tomli-w` 合成 `pixi.toml`。
   - lock 内容(若有)→ 写为 `pixi.lock`。
4. 运行 `pixi install --manifest-path <target_dir>/pixi.toml -e <environment>`;
   `locked=True` 追加 `--locked`/`--frozen`;再追加 `pixi_install_options`。
   通过 `ray._private.runtime_env.utils.check_output_cmd` 执行并捕获日志。
5. 失败:`shutil.rmtree(target_dir, ignore_errors=True)` 后抛出,带 pixi stderr。
6. 返回 `get_directory_size_bytes(target_dir)`。

并发与缓存复用 `UvPlugin` 模式(per-URI 锁、已建哈希字节缓存、可取消任务)。

## `modify_context()` 与「清单如何到达 worker」(关键)

**约束**:`uv` 字段只是包名列表,无本地文件;pixi **path 模式**的清单位于 driver
文件系统,远程 worker 节点的 agent 读不到。插件字段又没有 driver 端 transform
钩子。

**解法** —— `create()` 按以下顺序定位清单:

1. dict 内联内容(`manifest_content`,由 `pixi()` 在 driver 端读好):**推荐路径**,
   多节点安全。
2. 相对 `working_dir` 的路径:`working_dir` 已物化到 worker,可在其中找到清单。
3. agent 节点本地绝对路径:单机 / 共享文件系统 / 镜像内置清单场景。

**`modify_context()`**:

- 解析 pixi 可执行(同 create 逻辑)。
- 先校验 `<target_dir>/.pixi/envs/<environment>` 存在,否则抛 `ValueError`
  (提示安装可能失败)。
- 设 `context.py_executable =
  "<pixi> run --manifest-path <target_dir>/pixi.toml -e <environment> python"`。
  环境激活(PATH、`LD_LIBRARY_PATH`、conda 钩子、env vars)完全交给 `pixi run`,
  无需手动拼 activation。

## 插件注册

- 通过 `RAY_RUNTIME_ENV_PLUGINS='[{"class":"ray_pixi.PixiPlugin"}]'` 注册,**必须
  在每个节点的 runtime_env_agent 上设置**(`ray start` / `ray.init()` 之前)。
- `plugin_config()` 返回该配置 dict;README 文档化三种设置场景:cluster YAML、
  `ray start`、单机 `ray.init()`。

## 错误处理

| 情况 | 行为 |
| --- | --- |
| 无 pixi 且未设 `pixi_version` | `RuntimeError`,提示安装 pixi 或指定版本 |
| `pixi install` 失败 | 清理 `target_dir` + 透出 pixi stderr |
| 清单无法定位 | `ValueError`,提示用 `pixi()` 或放进 working_dir |
| 字段非法(两种来源都给/都没给) | `ValueError`(在 `validate()`/`_spec.py`) |
| 装完 env 目录不存在 | `ValueError` |

## 测试策略(TDD)

- **单元测试**(无需 Ray 集群):
  - `_spec.py`:字段解析/校验、互斥规则、URI 哈希确定性与失效。
  - `_manifest.py`:dict → `pixi.toml` 合成、内联内容写盘。
  - `_binary.py`:PATH 查找、`pixi_version` 引导逻辑(下载部分用桩/标记)。
  - `pixi()`:读文件并内联、内联 spec 规范化。
- **集成测试**(可选、需 pixi + 本地 Ray,缺失则 skip):
  `ray.init(runtime_env={"pixi": ...})` 跑一个 task,断言 `sys.executable` 来自
  pixi env 且能 import 已装包。保持最小。

## 依赖

- 运行时新增 `tomli-w`(写合成的 `pixi.toml`;读用 stdlib `tomllib`)。
- 已有 `ray>=2.50`。
- pixi 二进制:运行时要求 PATH 上有 `pixi`,或通过 `pixi_version` 引导下载。

## 开放问题 / 后续扩展

- driver 端 `pixi run` 自动探测 hook(对应 uv hook),作为后续独立 spec。
- 可选的 ray 版本兼容性检查(目前仅文档说明,不主动注入)。
- 可选随包附带 `pixi` JSON schema 以获得更友好的 driver 端校验。
