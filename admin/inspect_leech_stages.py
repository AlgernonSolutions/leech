from toll_booth.alg_obj.aws.sapper.dynamo_scanner import DynamoScanner


def show_leech_stages():
    scanner = DynamoScanner(None)
    leech_stages = scanner.scan_leech_stages()
    for stage_name, sid_values in leech_stages.items():
        print(f'{stage_name}: {len(sid_values)}')


if __name__ == '__main__':
    show_leech_stages()
