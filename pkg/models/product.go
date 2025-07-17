package models

type Product struct {
	ID    string  `json:"id" csv:"id"`
	Name  string  `json:"name" csv:"name"`
	Image string  `json:"image" csv:"image"`
	Price float64 `json:"price" csv:"price"`
	Qty   int     `json:"quantity" csv:"quantity"`
}
