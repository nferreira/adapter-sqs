package fiber

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"time"

	f "github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/nferreira/adapter/pkg/adapter"
	"github.com/nferreira/app/pkg/app"
	"github.com/nferreira/app/pkg/env"
	"github.com/nferreira/app/pkg/service"
)

const (
	Headers       = service.HeadersField
	Body          = service.BodyField
	CorrelationId = "Correlation-Id"
	AdapterId     = "fiber"
)

var (
	ErrBadPayload = errors.New("Bad payload")
)

type Params map[string]interface{}
type Handler func(path string, handlers ...f.Handler) f.Router
type GetParams func(fiberRule *BindingRule,
	businessService *service.BusinessService,
	c *f.Ctx) (*Params, error)

type Adapter struct {
	app      app.App
	fiberApp *f.App
}

func New() adapter.Adapter {
	return &Adapter{
		app:      nil,
		fiberApp: newFiber(),
	}
}

func (a *Adapter) BindRules(rules map[adapter.BindingRule]service.BusinessService) {
	for rule, businessService := range rules {
		fiberRule := rule.(*BindingRule)
		if fiberRule.Method == Get {
			bind(fiberRule, a, &businessService, a.fiberApp.Get, getParams)
		} else if fiberRule.Method == Post {
			bind(fiberRule, a, &businessService, a.fiberApp.Post, getPayload)
		} else if fiberRule.Method == Put {
			bind(fiberRule, a, &businessService, a.fiberApp.Put, getPayload)
		} else if fiberRule.Method == Patch {
			bind(fiberRule, a, &businessService, a.fiberApp.Patch, getPayload)
		} else if fiberRule.Method == Delete {
			bind(fiberRule, a, &businessService, a.fiberApp.Delete, getParams)
		} else if fiberRule.Method == Options {
			bind(fiberRule, a, &businessService, a.fiberApp.Options, getParams)
		}
	}
}

func (a *Adapter) Start(ctx context.Context) error {
	application := ctx.Value("app")
	switch application.(type) {
	case app.App:
		a.app = application.(app.App)
	default:
		panic("I need an application to live!!!")
	}
	return a.fiberApp.Listen(env.GetString("FIBER_HTTP_PORT", ":8080"))
}

func (a *Adapter) Stop(_ context.Context) error {
	return a.fiberApp.Shutdown()
}

func (a *Adapter) CheckHealth(ctx context.Context) error {
	return nil
}

func newFiber() *f.App {
	fiberApp := f.New(f.Config{
		Concurrency:     env.GetInt("FIBER_CONCURRENCY", 256*1024),
		ReadTimeout:     env.GetDuration("FIBER_READ_TIMEOUT", time.Minute, time.Duration(3)*time.Minute),
		WriteTimeout:    env.GetDuration("FIBER_WRITER_TIMEOUT", time.Minute, time.Duration(3)*time.Minute),
		ReadBufferSize:  env.GetInt("FIBER_READ_BUFFER", 4096),
		WriteBufferSize: env.GetInt("FIBER_WRITE_BUFFER", 4096),
	})

	loggerMiddleware := logger.New(logger.Config{
		Next:         nil,
		Format:       "[${status} - ${latency} ${method} ${path}\n",
		TimeFormat:   "15:04:05",
		TimeZone:     "Local",
		TimeInterval: 500 * time.Millisecond,
		Output:       os.Stderr,
	})

	fiberApp.Use(func(ctx *f.Ctx) error {
		if ctx.Path() != "/health" {
			return loggerMiddleware(ctx)
		}
		return nil
	})

	if env.GetBool("FIBER_USE_COMPRESSION", false) {
		fiberApp.Use(compress.New(compress.Config{
			Level: compress.LevelBestSpeed,
		}))
	}

	return fiberApp
}

func getParams(fiberRule *BindingRule, businessService *service.BusinessService, c *f.Ctx) (*Params, error) {
	params := make(Params)
	for _, param := range fiberRule.Params {
		var value string
		value = c.Params(param)
		if strings.TrimSpace(value) == "" {
			value = strings.TrimSpace(c.Query(param))
		}
		params[param] = value
	}
	return &params, nil
}

func getPayload(fiberRule *BindingRule, businessService *service.BusinessService, c *f.Ctx) (params *Params, err error) {
	params, err = getParams(fiberRule, businessService, c)
	if err != nil {
		return nil, err
	}
	serviceRequest := (*businessService).CreateRequest()
	headers := make(map[string]string)
	c.Request().Header.VisitAll(func(key []byte, value []byte) {
		headers[string(key)] = string(value)
	})
	(*params)[Headers] = headers
	err = c.BodyParser(&serviceRequest)
	if err == nil {
		(*params)[Body] = serviceRequest
		return params, nil
	}
	return nil, ErrBadPayload
}

func bind(fiberRule *BindingRule,
	a *Adapter,
	businessService *service.BusinessService,
	handler Handler,
	getParams GetParams) {

	handler(fiberRule.Path, func(c *f.Ctx) error {
		params, err := getParams(fiberRule, businessService, c)
		if err != nil {
			a.handleResult(c,
				service.
					NewResultBuilder().
					WithError(err).
					Build(),
				fiberRule)
			return err
		}

		result, done := a.executeBusinessService(c, businessService, params, fiberRule)
		if done {
			return nil
		}
		a.handleResult(c, result, fiberRule)
		return nil
	})
}

func (a *Adapter) executeBusinessService(c *f.Ctx, businessService *service.BusinessService, params *Params, fiberRule *BindingRule) (*service.Result, bool) {
	correlationId := GetCorrelationId(c)
	executionContext := service.NewExecutionContext(correlationId, a.app)
	ctx := context.WithValue(c.Context(), service.ExecutionContextKey, executionContext)
	p := service.Params(map[string]interface{}(*params))
	result := (*businessService).Execute(ctx, &p)
	if result.Error != nil {
		k := reflect.TypeOf(result.Error).Kind()
		hashable := k < reflect.Array || k == reflect.Ptr || k == reflect.UnsafePointer
		if hashable {
			status, found := fiberRule.ErrorMapping[result.Error]
			if found {
				_ = c.SendStatus(status)
			} else {
				if result.Code == 0 {
					_ = c.SendStatus(500)
				} else {
					_ = c.SendStatus(result.Code)
				}
			}
		} else {
			if result.Code == 0 {
				_ = c.SendStatus(500)
			} else {
				_ = c.SendStatus(result.Code)
			}
		}

		return result, true
	}
	return result, false
}

func (a *Adapter) handleResult(c *f.Ctx, result *service.Result, fiberRule *BindingRule) {
	if result.Error != nil {
		status, found := fiberRule.ErrorMapping[result.Error]
		if found {
			_ = c.SendStatus(status)
		} else {
			if result.Code == 0 {
				_ = c.SendStatus(500)
			} else {
				_ = c.SendStatus(result.Code)
			}
		}
	} else {
		if result.Code == 0 {
			_ = c.SendStatus(500)
		} else {
			c.Status(result.Code)
		}
		for key, value := range result.Headers {
			v, err := ToString(value)
			if err != nil {
				_ = c.Status(500).SendString(err.Error())
				return
			}
			c.Set(key, *v)
		}
		if result.Response != nil {
			err := c.JSON(result.Response)
			if err != nil {
				_ = c.SendStatus(500)
			}
		}
	}
}
