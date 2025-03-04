from quart import abort, jsonify, Response

import payment.payment_logic as logic
from payment.app_instance import app

DB_ERROR_STR = 'DB error'
REQ_ERROR_STR = 'Requests error'


@app.post('/create_user')
async def create_user():
    uuid, err = await logic.create_user()

    if err:
        return abort(400, DB_ERROR_STR)

    return jsonify({'user_id': uuid})


@app.post('/payment/batch_init/<int:n>/<int:starting_money>')
async def batch_init_users(n: int, starting_money: int):
    msg, err = await logic.multi_set_user(n, starting_money)

    if err:
        return abort(400, DB_ERROR_STR)

    return jsonify({'msg': 'Batch init for users successful'})


@app.get('/find_user/<user_id>')
async def find_user(user_id: str):
    user_entry, err = await logic.get_user(user_id)

    if err:
        abort(400, DB_ERROR_STR)

    return jsonify(
        {
            'user_id': user_id,
            'credit': user_entry.credit
        }
    )


@app.post('/payment/add_funds/<user_id>/<int:amount>')
async def add_credit(user_id: str, amount: int):
    credit, err = await logic.add_credit(user_id, amount)

    if err:
        return abort(400, DB_ERROR_STR)

    return Response(f"User: {user_id} credit updated to: {credit}", status=200)


@app.post('/payment/pay/<user_id>/<int:amount>')
async def remove_credit(user_id: str, amount: int):
    credit, err = await logic.remove_credit(user_id, amount)

    if isinstance(err, logic.NotEnoughCreditError):
        return abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    elif isinstance(err, logic.DBError):
        return abort(400, DB_ERROR_STR)

    return Response(f"User: {user_id} credit updated to: {credit}", status=200)
