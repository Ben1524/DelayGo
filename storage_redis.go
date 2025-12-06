package delaygo

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// RedisStorage Redis 存储实现
type RedisStorage struct {
	client *redis.Client
	prefix string
}

// RedisStorageOption Redis 存储配置选项
type RedisStorageOption func(*RedisStorage)

// WithRedisPrefix 设置 Redis Key 前缀
func WithRedisPrefix(prefix string) RedisStorageOption {
	return func(s *RedisStorage) {
		s.prefix = prefix
	}
}

// NewRedisStorage 创建 Redis 存储
func NewRedisStorage(client *redis.Client, opts ...RedisStorageOption) *RedisStorage {
	s := &RedisStorage{
		client: client,
		prefix: "delaygo",
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *RedisStorage) keyJob(id uint64) string {
	return fmt.Sprintf("%s:job:%d", s.prefix, id)
}

func (s *RedisStorage) keyIDs() string {
	return fmt.Sprintf("%s:ids", s.prefix)
}

func (s *RedisStorage) keyTopic(topic string) string {
	return fmt.Sprintf("%s:topic:%s", s.prefix, topic)
}

func (s *RedisStorage) keyState(state DelayState) string {
	return fmt.Sprintf("%s:state:%d", s.prefix, state)
}

// SaveDelayJob 保存完整任务
func (s *RedisStorage) SaveDelayJob(ctx context.Context, meta *DelayJobMeta, body []byte) error {
	jobKey := s.keyJob(meta.ID)

	// 检查是否存在
	exists, err := s.client.Exists(ctx, jobKey).Result()
	if err != nil {
		return err
	}
	if exists > 0 {
		return ErrDelayJobExists
	}

	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	pipe := s.client.TxPipeline()

	// 保存 Job Hash (m=meta, b=body, t=topic, s=state)
	// 存储 topic 和 state 是为了方便 Update 时维护索引
	pipe.HSet(ctx, jobKey,
		"m", metaBytes,
		"b", body,
		"t", meta.Topic,
		"s", int(meta.DelayState),
	)

	// 更新索引
	score := float64(meta.ID)
	pipe.ZAdd(ctx, s.keyIDs(), redis.Z{Score: score, Member: meta.ID})
	pipe.ZAdd(ctx, s.keyTopic(meta.Topic), redis.Z{Score: score, Member: meta.ID})
	pipe.ZAdd(ctx, s.keyState(meta.DelayState), redis.Z{Score: score, Member: meta.ID})

	_, err = pipe.Exec(ctx)
	return err
}

// UpdateDelayJobMeta 更新任务元数据
func (s *RedisStorage) UpdateDelayJobMeta(ctx context.Context, meta *DelayJobMeta) error {
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	// 使用 Lua 脚本原子更新索引
	// KEYS[1] = jobKey
	// KEYS[2] = keyIDs (not used but consistent)
	// ARGV[1] = newMeta
	// ARGV[2] = newTopic
	// ARGV[3] = newState
	// ARGV[4] = jobID
	// ARGV[5] = prefix
	script := redis.NewScript(`
		local jobKey = KEYS[1]
		local newMeta = ARGV[1]
		local newTopic = ARGV[2]
		local newState = tonumber(ARGV[3])
		local id = ARGV[4]
		local prefix = ARGV[5]

		-- 检查任务是否存在
		if redis.call("EXISTS", jobKey) == 0 then
			return redis.error_reply("delaygo: delayJob not found")
		end

		-- 获取旧的 topic 和 state
		local oldTopic = redis.call("HGET", jobKey, "t")
		local oldState = tonumber(redis.call("HGET", jobKey, "s"))

		-- 更新 Hash
		redis.call("HSET", jobKey, "m", newMeta, "t", newTopic, "s", newState)

		-- 更新 Topic 索引
		if oldTopic ~= newTopic then
			redis.call("ZREM", prefix .. ":topic:" .. oldTopic, id)
			redis.call("ZADD", prefix .. ":topic:" .. newTopic, id, id)
		end

		-- 更新 State 索引
		if oldState ~= newState then
			redis.call("ZREM", prefix .. ":state:" .. oldState, id)
			redis.call("ZADD", prefix .. ":state:" .. newState, id, id)
		end

		return "OK"
	`)

	keys := []string{s.keyJob(meta.ID)}
	args := []interface{}{
		string(metaBytes),
		meta.Topic,
		int(meta.DelayState),
		meta.ID,
		s.prefix,
	}

	_, err = script.Run(ctx, s.client, keys, args...).Result()
	if err != nil && err.Error() == "delaygo: delayJob not found" {
		return ErrNotFound
	}
	return err
}

// GetDelayJobMeta 获取任务元数据
func (s *RedisStorage) GetDelayJobMeta(ctx context.Context, id uint64) (*DelayJobMeta, error) {
	data, err := s.client.HGet(ctx, s.keyJob(id), "m").Bytes()
	if err == redis.Nil {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	var meta DelayJobMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// GetDelayJobBody 获取任务 Body
func (s *RedisStorage) GetDelayJobBody(ctx context.Context, id uint64) ([]byte, error) {
	data, err := s.client.HGet(ctx, s.keyJob(id), "b").Bytes()
	if err == redis.Nil {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

// DeleteDelayJob 删除任务
func (s *RedisStorage) DeleteDelayJob(ctx context.Context, id uint64) error {
	// 使用 Lua 脚本原子删除
	script := redis.NewScript(`
		local jobKey = KEYS[1]
		local id = ARGV[1]
		local prefix = ARGV[2]

		if redis.call("EXISTS", jobKey) == 0 then
			return redis.error_reply("delaygo: delayJob not found")
		end

		local topic = redis.call("HGET", jobKey, "t")
		local state = redis.call("HGET", jobKey, "s")

		redis.call("DEL", jobKey)
		redis.call("ZREM", prefix .. ":ids", id)
		if topic then
			redis.call("ZREM", prefix .. ":topic:" .. topic, id)
		end
		if state then
			redis.call("ZREM", prefix .. ":state:" .. state, id)
		end

		return "OK"
	`)

	keys := []string{s.keyJob(id)}
	args := []interface{}{id, s.prefix}

	_, err := script.Run(ctx, s.client, keys, args...).Result()
	if err != nil && err.Error() == "delaygo: delayJob not found" {
		return ErrNotFound
	}
	return err
}

// ScanDelayJobMeta 扫描任务元数据
func (s *RedisStorage) ScanDelayJobMeta(ctx context.Context, filter *DelayJobMetaFilter) (*DelayJobMetaList, error) {
	if filter == nil {
		filter = &DelayJobMetaFilter{}
	}

	var zkey string
	if filter.Topic != "" {
		zkey = s.keyTopic(filter.Topic)
	} else if filter.DelayState != nil {
		zkey = s.keyState(*filter.DelayState)
	} else {
		zkey = s.keyIDs()
	}

	// ZRANGEBYSCORE (cursor, +inf LIMIT 0 limit
	min := "(" + strconv.FormatUint(filter.Cursor, 10)
	max := "+inf"

	limit := int64(filter.Limit)
	if limit <= 0 {
		limit = 100 // 默认限制
	}

	ids, err := s.client.ZRangeByScore(ctx, zkey, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: 0,
		Count:  limit,
	}).Result()
	if err != nil {
		return nil, err
	}

	metas := make([]*DelayJobMeta, 0, len(ids))
	var nextCursor uint64

	// 批量获取 Meta
	if len(ids) > 0 {
		pipe := s.client.Pipeline()
		cmds := make([]*redis.StringCmd, len(ids))
		for i, idStr := range ids {
			id, _ := strconv.ParseUint(idStr, 10, 64)
			cmds[i] = pipe.HGet(ctx, s.keyJob(id), "m")
			nextCursor = id
		}
		_, err = pipe.Exec(ctx)
		if err != nil && err != redis.Nil {
			return nil, err
		}

		for i, cmd := range cmds {
			data, err := cmd.Bytes()
			if err == redis.Nil {
				// 索引存在但数据不存在？可能是并发删除，忽略
				continue
			}
			if err != nil {
				return nil, err
			}

			var meta DelayJobMeta
			if err := json.Unmarshal(data, &meta); err != nil {
				return nil, err
			}

			// 二次过滤 (如果使用了 Topic 索引但还需要过滤 State，或者反之)
			if filter.Topic != "" && meta.Topic != filter.Topic {
				continue
			}
			if filter.DelayState != nil && meta.DelayState != *filter.DelayState {
				continue
			}

			metas = append(metas, &meta)

			// 更新 nextCursor 为当前处理的 ID
			// 注意：如果因为二次过滤跳过了，nextCursor 仍然应该推进，否则会死循环
			// 但这里 ids 是有序的，所以 nextCursor 总是指向当前批次最大的 ID
			id, _ := strconv.ParseUint(ids[i], 10, 64)
			nextCursor = id
		}
	}

	return &DelayJobMetaList{
		Metas:      metas,
		HasMore:    len(ids) == int(limit),
		NextCursor: nextCursor,
	}, nil
}

// CountDelayJobs 统计任务数量
func (s *RedisStorage) CountDelayJobs(ctx context.Context, filter *DelayJobMetaFilter) (int, error) {
	if filter == nil {
		count, err := s.client.ZCard(ctx, s.keyIDs()).Result()
		return int(count), err
	}

	if filter.Topic != "" {
		count, err := s.client.ZCard(ctx, s.keyTopic(filter.Topic)).Result()
		return int(count), err
	}

	if filter.DelayState != nil {
		count, err := s.client.ZCard(ctx, s.keyState(*filter.DelayState)).Result()
		return int(count), err
	}

	// 如果有复杂过滤，Redis 难以直接 Count，这里简化处理返回总数
	// 或者遍历 (性能差)
	return 0, nil
}

// GetMaxDelayJobID 获取最大任务 ID
func (s *RedisStorage) GetMaxDelayJobID(ctx context.Context) (uint64, error) {
	// ZREVRANGE key 0 0
	ids, err := s.client.ZRevRange(ctx, s.keyIDs(), 0, 0).Result()
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	return strconv.ParseUint(ids[0], 10, 64)
}

// Close 关闭存储
func (s *RedisStorage) Close() error {
	return s.client.Close()
}
