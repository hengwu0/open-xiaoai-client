// AppError 是整个项目统一使用的错误别名。
//
// 当前直接复用 anyhow::Error，目的是让各层都能方便地：
// - 透传底层错误
// - 补充上下文
// - 在模块边界之间少写样板错误类型转换
pub type AppError = anyhow::Error;
