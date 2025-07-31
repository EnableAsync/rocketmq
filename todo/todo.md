# 消息过期
先加到缓存再释放锁

# 消息确认
offset 的提交：commitAndNext 的时候应该返回当前未被 ack 的最小的 offset