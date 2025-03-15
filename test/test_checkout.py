import utils as tu

def main():
    # 1. Crea un utente
    user = tu.create_user()
    user_id = user["user_id"]
    print(f"User creato: {user_id}")

    # 2. Crea un item con prezzo 5
    item = tu.create_item(5)
    item_id = item["item_id"]
    print(f"Item creato: {item_id} con prezzo 5")

    # 3. Crea un ordine per l'utente appena creato
    order = tu.create_order(user_id)
    order_id = order["order_id"]
    print(f"Ordine creato: {order_id} per l'utente {user_id}")

    # 4. Aggiungi 10 unità di stock all'item
    add_stock_status = tu.add_stock(item_id, 10)
    print(f"Stock aggiornato per l'item {item_id} (10 unità): status {add_stock_status}")

    # 5. Aggiungi 5 unità dell'item all'ordine
    add_item_status = tu.add_item_to_order(order_id, item_id, 5)
    print(f"Aggiunte 5 unità dell'item {item_id} all'ordine {order_id}: status {add_item_status}")

    # 6. Aggiungi 100 fondi all'utente
    add_credit_status = tu.add_credit_to_user(user_id, 100)
    print(f"Aggiunti 100 fondi all'utente {user_id}: status {add_credit_status}")

    # 7. Esegui il checkout dell'ordine
    checkout_response = tu.checkout_order(order_id)
    print(f"Checkout dell'ordine {order_id}: {checkout_response.text}")

if __name__ == '__main__':
    main()
