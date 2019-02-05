from toll_booth.alg_obj.posts.accountant import filters
from toll_booth.alg_obj.posts.accountant.books import Workbook, WorkbookSheet


class ReportGenerator:
    @classmethod
    def generate_report(cls, queried_data, filter_rules):
        report_data = []
        for filter_rule in filter_rules:
            filtered_data = cls._filter(filter_rule, queried_data)
            report_data.append(filtered_data)
        return report_data

    @classmethod
    def _filter(cls, filter_rule, queried_data):
        filter_fn = cls._find_filter(filter_rule)
        filtered_data = filter_fn(query_data=queried_data)
        return filtered_data

    @classmethod
    def _find_filter(cls, filter_rule):
        filter_modules = [filters]
        filter_name = filter_rule.fn_name
        for filter_module in filter_modules:
            try:
                filter_fn = getattr(filter_module, filter_name)
                return filter_fn
            except AttributeError:
                pass
        raise NotImplementedError(f'could not find filter function for filter named: {filter_name}')
