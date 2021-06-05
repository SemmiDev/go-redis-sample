package main

import (
	"context"
	"fmt"
	cache "github.com/go-redis/cache/v8"
	redis "github.com/go-redis/redis/v8"
	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"
	"log"
	"os"
	"strconv"
	"time"
)

type Config struct {
	RedisHost     string
	RedisPassword string
	RedisPort     int
	RedisDatabase int
	RedisPoolSize int
	RedisTTL      time.Duration
}

func logIfErrNotNil(err error, message string) {
	if err != nil {
		log.Println(message)
	}
}
func toInt(data string) int {
	dataInInt, err := strconv.Atoi(data)
	if err != nil {
		return 0
	}
	return dataInInt
}
func toDuration(data string) time.Duration {
	dataInDuration, err := time.ParseDuration(data)
	if err != nil {
		return 0
	}
	return dataInDuration
}

func load() Config {
	err := godotenv.Load()
	logIfErrNotNil(err, "Error loading .env file")

	redisHost     := os.Getenv("RedisHost")
	redisPort     := toInt(os.Getenv("RedisPort"))
	redisPassword := os.Getenv("RedisPassword")
	redisDatabase := toInt(os.Getenv("RedisDatabase"))
	redisPoolSize := toInt(os.Getenv("RedisPoolSize"))
	redisTTL      := toDuration(os.Getenv("RedisTTL"))
	
	return Config{redisHost, redisPassword, redisPort, redisDatabase, redisPoolSize, redisTTL}
}

var config = load()

func Cfg() *Config {
	return &config
}

type Client interface {
	Conn() *redis.Client
	Cache() *cache.Cache
	Close() error
}

type client struct {
	db      *redis.Client
	dbcache *cache.Cache
}

func NewClient() (Client, error) {
	db := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d",
			Cfg().RedisHost,
			Cfg().RedisPort),
		Password: Cfg().RedisPassword,
		DB:       Cfg().RedisDatabase,
		PoolSize: Cfg().RedisPoolSize,
	})

	err := db.Ping(context.Background()).Err()
	if err != nil {
		return nil, err
	}

	dbcache := cache.New(&cache.Options{
		Redis:      db,
		LocalCache: cache.NewTinyLFU(1000, time.Minute),
	})

	return &client{db, dbcache}, nil
}

func (c *client) Conn() *redis.Client {
	return c.db
}
func (c *client) Cache() *cache.Cache {
	return c.dbcache
}
func (c *client) Close() error {
	return c.db.Close()
}

type RedisRepo interface {
	STORE(c context.Context, id int, data string)
	GET(c context.Context, id int) (string, error)
	DELETE(c context.Context, id int) error
}

type redisRepository struct {
	redisClient Client
}

func newRedisRepository(redisClient Client) RedisRepo {
	return &redisRepository{redisClient: redisClient}
}

func (r *redisRepository) STORE(c context.Context, id int, data string)  {
	r.redisClient.Cache().Set(
		&cache.Item{
			Ctx: c,
			Key: fmt.Sprintf("data_%d", id),
			Value: data,
			TTL: Cfg().RedisTTL,
		},
	)
}

func (r *redisRepository) GET(c context.Context, id int) (string, error) {
	var data = new(string)
	err := r.redisClient.Cache().Get(c, fmt.Sprintf("data_%d", id), data)
	if err != nil && err != cache.ErrCacheMiss {
		return "", err
	}
	return *data, nil
}

func (r *redisRepository) DELETE(c context.Context, id int) error {
	err := r.redisClient.Cache().Delete(
		c,
		fmt.Sprintf("data_%d", id))
	if err != nil && err != cache.ErrCacheMiss {
		return err
	}

	return nil
}

func main() {
	redisClient, err := NewClient()
	if err != nil {
		panic("error occurred when initialize client in redis")
	}
	defer redisClient.Close()

	repo := newRedisRepository(redisClient)
	app := fiber.New()
	api := app.Group("/app")

	api.Get("/:id", func(c *fiber.Ctx) error {
		id, err := strconv.Atoi(c.Params("id"))
		logIfErrNotNil(err, "error get id from url")
		result, _ := repo.GET(c.Context(), id)
		return c.JSON(result)
	})
	api.Post("/:id/:value", func(c *fiber.Ctx) error {
		id, err := strconv.Atoi(c.Params("id"))
		logIfErrNotNil(err, "error get id from url")
		value := c.Params("value")

		repo.STORE(c.Context(), id, value)
		return c.JSON(fiber.Map{"status": "ok"})
	})
	api.Delete("/:id", func(c *fiber.Ctx) error {
		id, err := strconv.Atoi(c.Params("id"))
		logIfErrNotNil(err, "error get id from url")

		repo.DELETE(c.Context(), id)
		return c.JSON(fiber.Map{"status": "ok"})
	})

	app.Listen(":9090")
}