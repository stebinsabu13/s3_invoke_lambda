package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

var redisClient *redis.Client

func InitRedis(addr, password string, db int) error {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("error connecting to Redis: %v", err)
	}

	log.Println("Successfully connected to Redis")
	return nil
}

func GetRedis() *redis.Client {
	return redisClient
}
