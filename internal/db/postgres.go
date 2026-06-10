// Package db ارتباط با PostgreSQL را مدیریت می‌کند.
// تنها استفاده‌ی این پکیج: بارگذاری اولیه ClientConfigها برای bootstrap FSM.
package db

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type ClientConfigRow struct {
	ID                          int64
	SystemID                    string
	PasswordHash                string
	MaxConnections              int32
	TPSLimit                    int32
	SubmitRespMessageIDType     string
	DeliveryReportMessageIDType string
}

type Postgres struct{ db *sql.DB }

func New(dsn string) (*Postgres, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Postgres{db: db}, nil
}

func (p *Postgres) Close() { _ = p.db.Close() }

// LoadAll تمام ClientConfigهای active را می‌خواند.
// فقط در bootstrap اولیه cluster استفاده می‌شود.
func (p *Postgres) LoadAll() ([]ClientConfigRow, error) {
	rows, err := p.db.Query(`
		SELECT id, system_id, password_hash,
		       max_connections, tps_limit,
		       submit_resp_message_id_type,
		       delivery_report_message_id_type
		FROM   client_configs
		WHERE  active = TRUE
		ORDER  BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ClientConfigRow
	for rows.Next() {
		var r ClientConfigRow
		if err := rows.Scan(
			&r.ID, &r.SystemID, &r.PasswordHash,
			&r.MaxConnections, &r.TPSLimit,
			&r.SubmitRespMessageIDType,
			&r.DeliveryReportMessageIDType,
		); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}
