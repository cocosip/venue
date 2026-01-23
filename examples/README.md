# Venue 示例应用

本目录包含了Venue文件存储队列系统的示例应用，展示如何使用系统的各种功能。

## 示例列表

### 1. 简单示例 (`simple/`)

展示基本的文件上传和处理流程，适合快速入门。

**功能演示：**
- 系统初始化配置
- 创建租户
- 上传多个文件
- 顺序处理文件队列
- 查看系统统计信息

**运行方式：**
```bash
cd examples/simple
go run main.go
```

**输出示例：**
```
=== Venue Simple Example ===

1. 初始化配置...
   ✓ 数据目录: ./example-data

2. 初始化租户管理器...
   ✓ 创建租户: demo-tenant (状态: Enabled)

...

📤 上传文件...
   ✓ 文件 #1 已上传: 57955a1a-998f-4091-9591-74f70cf295a1 (document-1.txt, 71 字节)
   ✓ 文件 #2 已上传: 42163951-adf1-4f2a-820a-5db0e5fc3476 (document-2.txt, 71 字节)
   ...

⚙️  处理文件...
   处理中: 3657154c-50d3-43b5-a91a-172f3df157c9 (原始名: document-5.txt)
   ✓ 已完成: 3657154c-50d3-43b5-a91a-172f3df157c9
   ...

✅ 总共处理了 5 个文件
```

---

### 2. 并发处理示例 (`concurrent/`)

展示多个Worker并发处理文件的能力，适合生产环境场景。

**功能演示：**
- 批量上传50个文件
- 启动5个并发Worker
- 实时监控处理进度
- 优雅关闭Worker池
- 性能统计分析

**运行方式：**
```bash
cd examples/concurrent
go run main.go
```

**输出示例：**
```
=== Venue Concurrent Processing Example ===

🔧 初始化系统...
   ✓ 系统已初始化

📤 批量上传 50 个文件...
   ✓ 已上传 10/50 个文件
   ✓ 已上传 20/50 个文件
   ...
   ✓ 上传完成: 50/50 成功 (耗时: 58.4ms)

⚙️  启动 5 个并发worker处理文件...
   [Worker-1] 已启动
   [Worker-2] 已启动
   ...
   [Worker-1] ✓ 已处理: 0f4e4b65 (document-025.txt)
   [Worker-2] ✓ 已处理: 1e3c96f9 (document-023.txt)
   ...
   📊 进度: 50/50 已处理, 0 失败 (速率: 49.98 文件/秒)

🛑 停止workers...

📊 处理统计:
   总文件数: 50
   成功处理: 50
   处理失败: 0
   上传耗时: 58.4ms
   处理耗时: 1.0s
   平均速率: 49.98 文件/秒
```

**性能特点：**
- ⚡ 并发处理，提高吞吐量
- 🔄 自动负载均衡
- 📊 实时进度监控
- 🛡️ 优雅关闭机制

---

### 3. 后台清理服务示例 (`background-cleanup/`)

展示后台清理服务的自动清理功能，适合需要自动维护的生产环境。

**功能演示：**
- 自动清理空目录
- 超时文件重置
- 永久失败文件清理
- 数据库优化
- 优雅启动和关闭

**运行方式：**
```bash
cd examples/background-cleanup
go run main.go
```

**输出示例：**
```
=== Venue Background Cleanup Service Example ===

🔧 初始化系统...
   ✓ 系统已初始化

🚀 启动后台清理服务...
   ✓ 后台清理服务已启动

📝 提示:
   - 后台清理服务每 15 秒执行一次清理
   - 清理操作包括：空目录、超时文件、永久失败文件
   - 数据库优化每 1 分钟执行一次
   - 按 Ctrl+C 退出

2026/01/22 15:46:34 INFO Starting cleanup cycle
2026/01/22 15:46:34 INFO Cleaned up empty directories count=1
2026/01/22 15:46:34 INFO Starting database optimization
2026/01/22 15:46:34 INFO Database optimization completed
2026/01/22 15:46:34 INFO Cleanup cycle completed duration=10ms empty_dirs_removed=1 timed_out_reset=0 failed_removed=0 space_freed_bytes=0

^C
🛑 收到退出信号，正在优雅关闭...
✨ 后台清理服务示例完成！
```

**特点：**
- 🔄 自动清理，无需手动维护
- ⏰ 可配置清理间隔
- 📊 清理统计日志
- 🛡️ 优雅关闭机制

---

### 4. 文件监控器示例 (`file-watcher/`)

展示文件监控器（FileWatcher）的自动文件导入功能，适合需要监控目录并自动导入文件的场景。

**功能演示：**
- 自动监控目录
- 多租户模式（自动创建租户子目录）
- 文件过滤（类型、大小）
- 后台自动扫描
- 导入后操作（Delete/Move/Keep）

**运行方式：**
```bash
cd examples/file-watcher
go run main.go
```

**输出示例：**
```
=== Venue File Watcher Example ===

🔧 初始化系统...
   ✓ 创建租户: tenant-001
   ✓ 创建租户: tenant-002
   ✓ 创建租户: tenant-003
   ✓ 系统已初始化

📂 配置文件监控器...
   ✓ 多租户监控器已注册
   监控目录: ./watch/multi-tenant
   模式: 多租户（自动创建租户子目录）

🚀 启动后台文件监控服务...
   ✓ 后台文件监控服务已启动

📝 创建测试文件...
   ✓ 创建测试文件: test-file-1.txt (租户: tenant-001)
   ...

2026/01/22 15:56:05 INFO Watcher scan completed watcherID=watcher-multi-tenant discovered=9 imported=9 skipped=0 failed=0 bytes=333 duration=50ms
```

**特点：**
- 📁 自动监控目录
- 🏢 多租户支持
- 🔍 文件过滤
- 🔄 后台自动扫描
- 🎯 灵活的导入后操作

---

## 系统架构

所有示例都使用相同的系统架构：

```
┌─────────────────────────────────────────────┐
│   存储池 (StoragePool)                      │
│   - 统一的文件存储和队列接口                │
├─────────────────────────────────────────────┤
│   租户管理器 (TenantManager)                │
│   - 多租户隔离                              │
├─────────────────────────────────────────────┤
│   元数据仓库 (MetadataRepository)           │
│   - BadgerDB 持久化                         │
├─────────────────────────────────────────────┤
│   文件调度器 (FileScheduler)                │
│   - 队列管理和重试机制                      │
├─────────────────────────────────────────────┤
│   配额管理器 (QuotaManager)                 │
│   - 租户和目录级别配额                      │
├─────────────────────────────────────────────┤
│   清理服务 (CleanupService)                 │
│   - 超时重置、失败文件清理                  │
├─────────────────────────────────────────────┤
│   存储卷 (StorageVolume)                    │
│   - 本地文件系统                            │
└─────────────────────────────────────────────┘
```

## 核心概念

### 1. 文件生命周期

```
上传 → Pending → Processing → Completed (删除)
                    ↓
                  Failed → Retry (Pending)
                    ↓
              PermanentlyFailed → 清理
```

### 2. 文件Key（FileKey）

- 系统自动生成UUID格式
- 全局唯一标识符
- 不依赖原始文件名

### 3. 队列处理

- FIFO（先进先出）
- 原子操作，无并发冲突
- 自动重试机制

### 4. 多租户隔离

- 每个租户独立的元数据库
- 独立的配额管理
- 独立的存储路径

## 配置说明

示例使用的默认配置（来自 `internal/config`）：

```go
config.DefaultConfig()
// 包含以下配置：
// - 数据路径
// - 存储卷配置
// - 重试策略 (最多5次)
// - 处理超时 (30分钟)
// - 清理策略
```

## 自定义开发

基于这些示例，您可以：

1. **自定义处理逻辑** - 修改 `processFile()` 函数
2. **调整并发数** - 修改 `numWorkers` 参数
3. **配置存储路径** - 修改 `config` 中的路径设置
4. **添加错误处理** - 增强错误处理和日志记录
5. **集成监控** - 添加Prometheus指标或其他监控

## 常见问题

### Q: 为什么并发上传会失败？

A: BadgerDB在高并发写入时可能产生事务冲突。当前示例采用顺序上传，但处理是并发的。在生产环境中，建议：
- 使用消息队列进行上传解耦
- 或者实现重试机制处理事务冲突

### Q: 如何实现生产级部署？

A: 参考这些示例的架构，但需要：
- 使用持久化配置文件
- 添加完整的错误处理和日志
- 实现健康检查和监控
- 配置适当的资源限制（文件大小、配额等）
- 使用进程管理器（systemd, Docker等）

### Q: Worker数量如何选择？

A: Worker数量取决于：
- CPU核心数（建议 = CPU核心数）
- 处理任务的I/O密集程度
- 可用内存
- 建议从少量Worker开始，逐步增加并测试性能

## 下一步

查看以下文档了解更多信息：

- [系统架构](../doc/REQUIREMENTS_AND_PLAN.md)
- [API文档](../pkg/core/interfaces.go)
- [开发指南](../CLAUDE.md)

## 许可证

MIT License - 查看 [LICENSE](../LICENSE) 文件
