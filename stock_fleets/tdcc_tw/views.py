from tdcc_tw.helpers import ReloadTdccTW
from crawlers.models import StockTdccTW
from fastapi import Request, Response, status


async def read_tdcc(request: Request, response: Response, stock_id: str, start_date: str = None, end_date: str = None,
                    offset: int = 0,limit: int = 100000, recent: int = 1, fields: list = None):
    query_params = dict(request.query_params)
    query_params['model'] = StockTdccTW
    try:
        context = ReloadTdccTW(**query_params).group_data()
        return context
    except KeyError:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'error_msg': 'stock_id is not in db.'}
