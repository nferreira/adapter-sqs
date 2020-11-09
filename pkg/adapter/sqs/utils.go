package sqs

import (
	"errors"
	"fmt"
	"github.com/gofiber/utils"
	"strconv"
	"strings"

	f "github.com/gofiber/sqs/v2"
)

func ToString(value interface{}) (*string, error){
	v := ""
	switch value.(type) {
	case int: v = strconv.Itoa(value.(int))
	case float32: v = fmt.Sprintf("%.2f", value.(float32))
	case float64: v = fmt.Sprintf("%.2f", value.(float64))
	case string: v = value.(string)
	default:
		return nil, errors.New("Headers can only be of string type")
	}
	return &v, nil
}

func GetCorrelationId(c *f.Ctx) string {
	correlationId := c.Get(CorrelationId)
	if len(strings.TrimSpace(correlationId)) == 0 {
		correlationId = utils.UUID()
	}
	return correlationId
}
