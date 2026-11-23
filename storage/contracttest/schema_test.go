package contracttest

import (
	"sync"
	"testing"

	"github.com/loopopen/gap/storage/xgorm"
	mysqldriver "gorm.io/driver/mysql"
	postgresdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

func TestGormPayloadDataType(t *testing.T) {
	parsed, err := schema.Parse(&xgorm.Model{}, new(sync.Map), schema.NamingStrategy{})
	if err != nil {
		t.Fatal(err)
	}
	field := parsed.LookUpField("Payload")
	if field == nil {
		t.Fatal("payload field not found")
	}

	tests := []struct {
		name      string
		dialector gorm.Dialector
		want      string
	}{
		{
			name: "mysql with custom default string size",
			dialector: mysqldriver.New(mysqldriver.Config{
				DefaultStringSize: 256,
			}),
			want: "LONGTEXT",
		},
		{
			name:      "postgres",
			dialector: postgresdriver.New(postgresdriver.Config{}),
			want:      "TEXT",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := &gorm.DB{Config: &gorm.Config{Dialector: test.dialector}}
			m := migrator.Migrator{Config: migrator.Config{
				DB:        db,
				Dialector: test.dialector,
			}}
			if got := m.DataTypeOf(field); got != test.want {
				t.Fatalf("payload type = %q, want %q", got, test.want)
			}
		})
	}
}
