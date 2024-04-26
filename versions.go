package migrate

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Versions interface {
	Set(ctx context.Context, version *Version) error
	Current(ctx context.Context) (*Version, error)
	Get(ctx context.Context, id int) (*Version, error)
	Delete(ctx context.Context, id int) error
}

type versions struct {
	collection *mongo.Collection
}

func (v *versions) Set(ctx context.Context, version *Version) error {
	// TODO implement me
	panic("implement me")
}

func (v *versions) Current(ctx context.Context) (*Version, error) {
	// TODO implement me
	panic("implement me")
}

func (v *versions) Get(ctx context.Context, id int) (*Version, error) {
	filter := bson.D{{}}
	sort := bson.D{bson.E{Key: "_id", Value: -1}}
	options := options.FindOne().SetSort(sort)

	// find record with greatest id (assuming it`s latest also)
	result := m.db.Collection(m.versions).FindOne(context.TODO(), filter, options)
	err := result.Err()
	switch {
	case err == mongo.ErrNoDocuments:
		return 0, "", nil
	case err != nil:
		return 0, "", err
	}

	var rec Version
	if err := result.Decode(&rec); err != nil {
		return 0, "", err
	}

	return rec.Version, rec.Description, nil
}

func (v *versions) Delete(ctx context.Context, id int) error {
	// TODO implement me
	panic("implement me")
}

func NewVersions(collection *mongo.Collection) *versions {
	return &versions{collection: collection}
}
