from decimal import Decimal


async def test_create_product(client):
    response = await client.post(
        "/products",
        json={"name": "Widget", "price": "19.99", "stock": 100},
    )

    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "Widget"
    assert Decimal(data["price"]) == Decimal("19.99")
    assert data["stock"] == 100
    assert "id" in data
    assert "created_at" in data


async def test_list_products(client):
    await client.post("/products", json={"name": "A", "price": "1.00", "stock": 1})
    await client.post("/products", json={"name": "B", "price": "2.00", "stock": 2})

    response = await client.get("/products")

    assert response.status_code == 200
    products = response.json()
    assert len(products) == 2
    assert products[0]["name"] == "A"
    assert products[1]["name"] == "B"


async def test_negative_price_rejected(client):
    response = await client.post(
        "/products",
        json={"name": "Bad", "price": "-1.00", "stock": 10},
    )

    assert response.status_code == 422


async def test_negative_stock_rejected(client):
    response = await client.post(
        "/products",
        json={"name": "Bad", "price": "10.00", "stock": -1},
    )

    assert response.status_code == 422
