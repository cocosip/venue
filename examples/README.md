# Venue 示例

仓库包含两个可运行示例，均使用公共入口 `venue.NewVenue(*config.Config)`。

## 程序化配置

`simple-venue` 展示链式配置、实例生命周期、租户获取以及文件写入和读取：

```powershell
go run ./examples/simple-venue
```

核心配置方式：

```go
cfg := config.New().
    WithMetadataDirectory("./venue-data/metadata").
    WithQuotaDirectory("./venue-data/quota").
    WithVolumes(
        config.NewVolumeConfig().
            WithVolumeID("default-volume").
            WithMountPath("./venue-data/storage").
            WithShardingDepth(2),
    ).
    WithTenants(config.NewTenantConfig("demo-tenant"))

runtime, err := venue.NewVenue(cfg)
```

`Config` 以及 `VolumeConfig`、`TenantConfig`、`RetryPolicyConfig`、
`TenantManagerConfig`、`MetadataConfig`、`BadgerDBConfig`、
`FileWatcherConfig`、`CleanupConfig`、`OrphanRecoveryConfig` 和
`DatabaseHealthCheckConfig` 都支持链式配置。顶层的 `WithVolumes`、
`WithTenants`、`WithFileWatchers` 替换整个集合；需要追加时使用
`AddVolume`、`AddTenant`、`AddFileWatcher`。

租户 ID 会作为元数据文件名和物理存储目录段使用，因此所有入口都会先校验：
包含 `/`、`\`、`:`、控制字符、前导/尾随点或空格、Windows 设备名，或超过
128 字节的 ID 会被拒绝为 `core.ErrInvalidArgument`。

## Viper 适配

`viper-config` 展示由独立适配器加载 YAML、JSON 或应用已有的 Viper 节点：

```powershell
go run ./examples/viper-config
```

```go
cfg, err := viperconfig.LoadFromFile("venue-config-example.yaml")
runtime, err := venue.NewVenue(cfg)
```

依赖方向固定为 `viperconfig -> config`。基础 `config` 包不导入
Viper，也不读取文件；不使用适配器的应用不会被迫依赖 Viper API。

所有公共配置字段都提供 `json`、`yaml`、`mapstructure` 标签，因此应用也可
选择其他解析器绑定到 `config.Config`。`Logging` 是运行时对象，三个标签均为
`-`，必须在解析完成后由应用程序注入。

## 配置文件

- [`../venue-config-example.yaml`](../venue-config-example.yaml)：带 `venue` 根节点的完整示例。
- [`viper-config/venue-config.yaml`](viper-config/venue-config.yaml)：根节点配置示例。
- [`viper-config/venue-config.json`](viper-config/venue-config.json)：JSON 配置示例。

从文件绑定时，`enabled` 与 `includeSubdirectories` 这类布尔键必须显式写出：
源文件中缺失的布尔键与显式的 `false` 无法区分。Go 侧的
`config.NewFileWatcherConfig()` 会把它们默认设为 `true`。

示例会在本地创建临时数据目录，仅用于演示，不应直接作为生产目录规划；生成的
数据库与配置转储已被 `.gitignore` 排除。
