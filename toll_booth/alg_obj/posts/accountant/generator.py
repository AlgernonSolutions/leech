from toll_booth.alg_obj.posts.accountant.books import Workbook, WorkbookSheet


class WorkbookGenerator:
    @classmethod
    def create_workbook(cls, *args, **kwargs):
        workbook_name = kwargs['workbook_name']
        workbook = Workbook(workbook_name)
        for arg in args:
            sheet = WorkbookSheet(arg['sheet_name'])

    @classmethod
    def generate_from_template(cls, template_name, workbook_data):
        pass
