import utils as tu

def main():
    #Create User
    user = tu.create_user()
    user_id = user["user_id"]
    print(f"User creato: {user_id}")

    #Create Item with price 5
    item = tu.create_item(5)
    item_id = item["item_id"]
    print(f"Item creato: {item_id} con prezzo 5")

    #Create Order 
    order = tu.create_order(user_id)
    order_id = order["order_id"]
    print(f"Ordine creato: {order_id} per l'utente {user_id}")

    #Add Stock to Item (10 units)
    add_stock_status = tu.add_stock(item_id, 10)
    print(f"Stock aggiornato per l'item {item_id} (10 unità): status {add_stock_status}")

    #Add Item to Order (5 units)
    add_item_status = tu.add_item_to_order(order_id, item_id, 5)
    print(f"Aggiunte 5 unità dell'item {item_id} all'ordine {order_id}: status {add_item_status}")

    #ìAdd Credit to User (100)
    add_credit_status = tu.add_credit_to_user(user_id, 100)
    print(f"Aggiunti 100 fondi all'utente {user_id}: status {add_credit_status}")

    #Checkout Order
    checkout_response = tu.checkout_order(order_id)
    print(f"Checkout dell'ordine {order_id}: {checkout_response.text}")

if __name__ == '__main__':
    main()
