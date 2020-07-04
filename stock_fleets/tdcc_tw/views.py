from tdcc_tw.helpers import ReloadTdccTW, TdccStrategy
from crawlers.models import StockTdccTW
from fastapi import Request, Response, status


async def read_tdcc(request: Request, response: Response, stock_id: str, start_date: str = None, end_date: str = None,
                    offset: int = 0, limit: int = 100000, recent: int = 1, fields: list = None):
    query_params = dict(request.query_params)
    query_params['model'] = StockTdccTW
    try:
        context = ReloadTdccTW(**query_params).group_data()
        return context
    except KeyError:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'error_msg': 'stock_id is not in db.'}


async def tdcc_strategy(request: Request, response: Response, start_date: str = None, end_date: str = None,
                        offset: int = 0, limit: int = 100000, recent: int = 1, shp_rank_max: float = 1,
                        shp_rank_min: float = 0, bhp_rank_max: float = 1, bhp_rank_min: float = 0,
                        lhp_rank_max: float = 1, lhp_rank_min: float = 0, lp_rank_max: float = 1,
                        lp_rank_min: float = 0, issued_num_max: int = 10000000, issued_num_min: int = 0,
                        ps_rank_max: float = 1, ps_rank_min: float = 0):
    query_params = dict(request.query_params)
    data_query_params = {}
    for q in ['start_date', 'end_date', 'offset', 'limit', 'recent']:
        if q in query_params.keys():
            data_query_params[q] = query_params.pop(q)
    data_query_params['model'] = StockTdccTW
    try:
        context = TdccStrategy(**data_query_params).select_list(**query_params)
        return {'list1': context}
    except KeyError:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'error_msg': 'data is not in db.'}
