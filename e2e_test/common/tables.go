package common

import (
	"database/sql"
	"time"
)

type User struct {
	ID        int       `db:"id"`
	Name      string    `db:"name"`
	CreatedAt time.Time `db:"created_at"`
}

type Post struct {
	ID        int    `db:"id"`
	Title     string `db:"title"`
	Content   string `db:"content"`
	UserID    int    `db:"user_id"`
	Published bool   `db:"published"`
}

type Product struct {
	ID    int             `db:"id"`
	Name  string          `db:"name"`
	Price sql.NullFloat64 `db:"price"`
}

type Tag struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

type ProductTag struct {
	ProductID int       `db:"product_id"`
	TagID     int       `db:"tag_id"`
	CreatedAt time.Time `db:"created_at"`
}
