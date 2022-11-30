package dynstore

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"kvstore/kvstore"
	"time"
)

// Partial interface for the dynamo SDK.
// https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/#New
type dynamoClient interface {
	PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
}

// Internal marker for creating a "not exists" expression for dynamo.
type notExistsCondition struct{}

// New returns a kvstore.Store backed by DynamoDB. Values are encoded as
// JSON and stored as strings.
func New(client dynamoClient, tableName string) kvstore.Store {
	d := &dynstore{
		client:    client,
		tableName: tableName,
	}
	d.Helper.Getter = d.Get
	return d
}

type dynstore struct {
	kvstore.Helper
	client    dynamoClient
	tableName string
}

func (s *dynstore) Set(k string, v interface{}) error {
	return s.set(k, v, nil, 0)
}

func (s *dynstore) SetIf(k string, v interface{}, c interface{}) error {
	return s.set(k, v, c, 0)
}

func (s *dynstore) SetIfNotExists(k string, v interface{}) error {
	return s.set(k, v, notExistsCondition{}, 0)
}

func (s *dynstore) SetX(k string, v interface{}, x time.Duration) error {
	return s.set(k, v, nil, x)
}

func (s *dynstore) set(k string, v interface{}, c interface{}, x time.Duration) error {
	key, _ := dynamodbattribute.Marshal(k)
	updatedAt, _ := dynamodbattribute.Marshal(time.Now())
	value, err := dynamodbattribute.Marshal(v)
	if err != nil {
		return errors.New("failed to marshal value")
	}

	var exp expression.Expression
	var expError error

	if c == (notExistsCondition{}) {
		exp, expError = expression.NewBuilder().
			WithCondition(expression.AttributeNotExists(expression.Name("key"))).
			Build()
	} else if c != nil {
		exp, expError = expression.NewBuilder().
			WithCondition(expression.Name("value").Equal(expression.Value(c))).
			Build()
	}
	if expError != nil {
		return errors.New("failed to build conditional expression")
	}

	req := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"key":        key,
			"value":      value,
			"updated_at": updatedAt,
		},
		TableName:                 aws.String(s.tableName),
		ConditionExpression:       exp.Condition(),
		ExpressionAttributeNames:  exp.Names(),
		ExpressionAttributeValues: exp.Values(),
	}

	if x != 0 {
		req.Item["ttl"], _ = dynamodbattribute.Marshal(time.Now().Add(x).Unix())
	}

	_, err = s.client.PutItem(req)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				// Expected error if the condition fails.
				return kvstore.ErrMismatch
			}
			return errors.New("failed to store item")
		}
		return errors.New("failed to store item")
	}

	return nil
}

func (s *dynstore) Delete(k string) error {
	_, err := s.client.DeleteItem(&dynamodb.DeleteItemInput{
		Key:       map[string]*dynamodb.AttributeValue{"key": {S: aws.String(k)}},
		TableName: aws.String(s.tableName),
	})
	if err != nil {
		return errors.New("failed to delete entry")
	}
	return nil
}

func (s *dynstore) Get(k string, v interface{}) error {
	result, err := s.client.GetItem(&dynamodb.GetItemInput{
		Key:            map[string]*dynamodb.AttributeValue{"key": {S: aws.String(k)}},
		ConsistentRead: aws.Bool(true),
		TableName:      aws.String(s.tableName),
	})
	if err != nil {
		return errors.New("failed to store item")
	}

	// Dynamo may continue to return expired keys, so we need to filter if we care
	// about accuracy.
	if ttl, ok := result.Item["ttl"]; ok {
		var expires int64
		if err := dynamodbattribute.Unmarshal(ttl, &expires); err != nil {
			return errors.New("failed to unmarshal ttl")
		}
		if expires > 0 {
			t := time.Unix(expires, 0)
			if t.Before(time.Now()) {
				return kvstore.ErrKeyNotFound
			}
		}
	}

	if av, ok := result.Item["value"]; ok {
		if err := dynamodbattribute.Unmarshal(av, v); err != nil {
			return errors.New("failed to unmarshal value")
		}
	} else {
		// Dynamo returns empty results instead of an error.
		return kvstore.ErrKeyNotFound
	}

	return nil
}
