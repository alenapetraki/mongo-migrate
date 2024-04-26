// Package migrate allows to perform versioned migrations in your MongoDB.
package migrate

import (
	"context"
	"slices"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
)

type Migrator interface {
	Up(ctx context.Context, n int) error
	Down(ctx context.Context, n int) error
	Version(ctx context.Context) (*Version, error)
}

const (
	// AllAvailable used in "Up" or "Down" methods to run all available migrations.
	AllAvailable = -1

	_defaultMigrationsCollection = "migrations"
)

// migrator is type for performing migrations in provided database.
// Database versioned using dedicated collection.
// Each migration applying ("up" and "down") adds new document to collection.
// This document consists migration version, migration description and timestamp.
// Current database version determined as version in latest added document (biggest "_id") from collection mentioned above.
type migrator struct {
	versions   Versions
	migrations Migrations
}

type config struct {
	db             *mongo.Database
	collectionName string
}

type Option func(c *config)

// WithMigrationsCollection sets name of collection with migration information.
// Default value is "migrations".
func WithMigrationsCollection(name string) Option {
	return func(c *config) {
		c.collectionName = name
	}
}

func NewMigrate(db *mongo.Database, migrations []Migration, options ...Option) *migrator {
	cfg := &config{
		collectionName: _defaultMigrationsCollection,
	}
	for _, o := range options {
		o(cfg)
	}
	return &migrator{
		migrations: slices.Clone(migrations),
		versions:   NewVersions(db.Collection(cfg.collectionName)),
	}
}

// func (m *migrator) isCollectionExist(name string) (isExist bool, err error) {
// 	collections, err := m.getCollections()
// 	if err != nil {
// 		return false, err
// 	}
//
// 	for _, c := range collections {
// 		if name == c.Name {
// 			return true, nil
// 		}
// 	}
// 	return false, nil
// }
//
// func (m *migrator) createCollectionIfNotExist(name string) error {
// 	exist, err := m.isCollectionExist(name)
// 	if err != nil {
// 		return err
// 	}
// 	if exist {
// 		return nil
// 	}
//
// 	command := bson.D{bson.E{Key: "create", Value: name}}
// 	err = m.db.RunCommand(nil, command).Err()
// 	if err != nil {
// 		return err
// 	}
//
// 	return nil
// }
//
// func (m *migrator) getCollections() (collections []collectionSpecification, err error) {
// 	filter := bson.D{bson.E{Key: "type", Value: "collection"}}
// 	options := options.ListCollections().SetNameOnly(true)
//
// 	cursor, err := m.db.ListCollections(context.Background(), filter, options)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	if cursor != nil {
// 		defer func(cursor *mongo.Cursor) {
// 			curErr := cursor.Close(context.TODO())
// 			if curErr != nil {
// 				if err != nil {
// 					err = errors.Wrapf(curErr, "migrate: get collection failed: %s", err.Error())
// 				} else {
// 					err = curErr
// 				}
// 			}
// 		}(cursor)
// 	}
//
// 	for cursor.Next(context.TODO()) {
// 		var collection collectionSpecification
//
// 		err := cursor.Decode(&collection)
// 		if err != nil {
// 			return nil, err
// 		}
//
// 		collections = append(collections, collection)
// 	}
//
// 	if err := cursor.Err(); err != nil {
// 		return nil, err
// 	}
//
// 	return
// }

// Version returns current database version and comment.
func (m *migrator) Version(ctx context.Context) (*Version, error) {
	return m.versions.Current(ctx)
}

// SetVersion forcibly changes database version to provided.
func (m *migrator) SetVersion(ctx context.Context, version uint, description string) error {
	rec := Version{
		Version:     version,
		Timestamp:   time.Now(),
		Description: description,
	}

	_, err := m.db.Collection(m.versions).InsertOne(ctx, rec)
	if err != nil {
		return err
	}

	return nil
}

// Up performs "up" migrations to latest available version.
// If n<=0 all "up" migrations with newer versions will be performed.
// If n>0 only n migrations with newer version will be performed.
func (m *migrator) Up(ctx context.Context, n int) error {
	curVer, _, err := m.CurrentVersion(ctx)
	if err != nil {
		return err
	}

	if n <= 0 || n > len(m.migrations) {
		n = len(m.migrations)
	}

	m.migrations.Sort()

	for _, mn := range m.migrations {
		if n <= 0 {
			break
		}
		n--

		if mn.Version.ID <= curVer || mn.Up == nil {
			continue
		}
		if err = mn.Up(m.db); err != nil {
			return errors.Wrapf(err, "migrate on version '%d'", curVer)
		}
		if err = m.SetVersion(ctx, mn.Version, mn.Description); err != nil {
			return errors.Wrapf(err, "set version '%d' info", curVer)
		}
	}

	return nil
}

// Down performs "down" migration to oldest available version.
// If n<=0 all "down" migrations with older version will be performed.
// If n>0 only n migrations with older version will be performed.
func (m *migrator) Down(ctx context.Context, n int) error {
	curVer, _, err := m.CurrentVersion(ctx)
	if err != nil {
		return err
	}
	if n <= 0 || n > len(m.migrations) {
		n = len(m.migrations)
	}

	m.migrations.Sort(-1)

	for _, mn := range m.migrations {
		if n <= 0 {
			break
		}
		n--

		if mn.Version <= curVer || mn.Down == nil {
			continue
		}
		if err = mn.Down(m.db); err != nil {
			return errors.Wrapf(err, "migrate on version '%d'", curVer)
		}

		var prev Migration
		if i == 0 {
			prev = Migration{Version: 0}
		} else {
			prev = m.migrations[i-1]
		}

		if err = m.SetVersion(ctx, mn.Version, mn.Description); err != nil {
			return errors.Wrapf(err, "set version '%d' info", curVer)
		}
	}

	for i, p := len(m.migrations)-1, 0; i >= 0 && p < n; i-- {
		migration := m.migrations[i]
		if migration.Version > currentVersion || migration.Down == nil {
			continue
		}
		p++
		if err := migration.Down(m.db); err != nil {
			return err
		}

		var prevMigration Migration
		if i == 0 {
			prevMigration = Migration{Version: 0}
		} else {
			prevMigration = m.migrations[i-1]
		}
		if err := m.SetVersion(prevMigration.Version, prevMigration.Description); err != nil {
			return err
		}
	}
	return nil
}
