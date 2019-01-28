class Workbook:
    def __init__(self, workbook_name, workbook_sheets=None):
        if not workbook_sheets:
            workbook_sheets = []
        self._workbook_name = workbook_name
        self._workbook_sheets = workbook_sheets

    def add_sheet(self, workbook_sheet, **kwargs):
        if 'position' in kwargs:
            return self._workbook_sheets.insert(kwargs['position'], workbook_sheet)
        self._workbook_sheets.append(workbook_sheet)


class WorkbookSheet:
    def __init__(self, sheet_name, header=None, sheet_rows=None):
        if not sheet_rows:
            sheet_rows = []
        if not header:
            header = []
        self._sheet_name = sheet_name
        self._header = header
        self._sheet_rows = sheet_rows

    def add_header(self, header):
        self._header = header

    def add_sheet_rows(self, sheet_row, **kwargs):
        if 'position' in kwargs:
            return self._sheet_rows.insert(kwargs['position'], sheet_row)
        self._sheet_rows.append(sheet_row)


class WorkbookSheetRow:
    def __init__(self, cells=None):
        if not cells:
            cells = []
        self._cells = cells
