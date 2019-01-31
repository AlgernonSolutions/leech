from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_tasks.rivers.rocks import task


@xray_recorder.capture('send_email')
@task('send_email')
def send_email(**kwargs):
    raise NotImplementedError('you need to do this')


@xray_recorder.capture('build_reports')
@task('build_reports')
def build_reports(**kwargs):
    raise NotImplementedError('you need to do this')


@xray_recorder.capture('query_data')
@task('query_data')
def query_data(**kwargs):
    from toll_booth.alg_obj.posts.inquisitor import Inquisitor

    query_args = kwargs['query_args']
    query_name = kwargs['query_name']
    query_source = kwargs['query_source']

    query_results = Inquisitor.query_data(query_source, query_args)
    return {'query_data': {'query_name': query_name,  'query_results': query_results}}


@xray_recorder.capture('query_credible_data')
@task('query_credible_data')
def query_credible_data(**kwargs):
    from toll_booth.alg_obj.posts.inquisitor import Inquisitor

    query_args = kwargs['query_args']
    query_name = kwargs['query_name']
    query_source = kwargs['query_source']

    query_results = Inquisitor.query_data(query_source, query_args)
    return {'query_data': {'query_name': query_name,  'query_results': query_results}}


@xray_recorder.capture('get_report_args')
@task('get_report_args')
def get_report_args(**kwargs):
    from toll_booth.alg_obj.posts.report_schema import ReportSchema

    fns = {
        'get_date': _get_date
    }
    report_schema = ReportSchema.get(**kwargs)
    kwargs.update(fns)
    report_tags = kwargs['report_tags']
    report_name = kwargs['report_name']
    schema_report_args = report_schema.get_report_args(**kwargs)
    report_arg_set = schema_report_args[report_name]
    report_args = report_arg_set['default']
    for tag in report_tags:
        specified_args = report_arg_set[tag]
        report_args.update(specified_args)
    return {'report_args': report_args}


def _get_date(**kwargs):
    from datetime import datetime
    from datetime import timedelta

    if 'now' in kwargs:
        return datetime.utcnow()
    if 'from_now' in kwargs:
        return datetime.utcnow() - timedelta(days=int(kwargs['from_now']))
