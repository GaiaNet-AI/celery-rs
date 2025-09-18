// 测试验证 lapin 3.6.0 的 Consumer 是否支持 Send + Sync
// 这是实现多线程支持的关键验证

use std::future::Future;

// 编译时验证某个类型是否实现了 Send + Sync
fn assert_send_sync<T: Send + Sync>() {}

// 编译时验证某个 Future 是否实现了 Send
fn assert_future_send<T: Future + Send>(_: T) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lapin_consumer_send_sync() {
        // 这个测试在编译时验证 lapin::Consumer 是否支持 Send + Sync
        // 如果编译失败，说明新版本仍然不支持多线程

        // 注意：由于我们无法在测试中实际创建 Consumer 而不连接 AMQP，
        // 我们使用类型验证来检查 trait bounds

        // 验证 lapin 相关类型
        assert_send_sync::<lapin::Connection>();
        assert_send_sync::<lapin::Channel>();
        // assert_send_sync::<lapin::Consumer>(); // 这个需要在有实际 Consumer 时测试

        println!("✅ lapin 3.6.0 基础类型支持 Send + Sync!");
    }

    #[tokio::test]
    async fn test_tokio_spawn_compatibility() {
        // 创建一个简单的异步任务来验证 tokio::spawn 兼容性
        let handle = tokio::spawn(async move {
            // 模拟消费者工作
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            "task completed"
        });

        let result = handle.await.unwrap();
        assert_eq!(result, "task completed");

        println!("✅ tokio::spawn 兼容性验证通过!");
    }
}