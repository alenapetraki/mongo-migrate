package migrate

import (
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

type Version struct {
	ID          uint      `bson:"_id"`
	Description string    `bson:"description,omitempty"`
	Timestamp   time.Time `bson:"timestamp"`
}

// MigrationFn used to define actions to be performed during migration.
type MigrationFn func(db *mongo.Database) error

// Migration represents single database migration.
type Migration struct {
	Version
	Up   MigrationFn
	Down MigrationFn
}

type Migrations []Migration

func (ms Migrations) Sort(direction ...int) {
	if len(direction) > 0 && direction[0] == -1 {
		sort.Slice(ms, func(i, j int) bool { return ms[i].Version > ms[j].Version })
		return
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].Version < ms[j].Version })
}

func (ms Migrations) ContainsVersion(version uint) bool {
	for _, m := range ms {
		if m.Version == version {
			return true
		}
	}
	return false
}
