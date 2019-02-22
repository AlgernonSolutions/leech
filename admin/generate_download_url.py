from toll_booth.alg_obj.aws.snakes.invites import ObjectDownloadLink


def generate_signed_url(bucket_name, file_key):
    download_link = ObjectDownloadLink(bucket_name, file_key)
    return str(download_link)


if __name__ == '__main__':
    target_bucket_name = 'algernonsolutions-leech'
    target_file_key = 'ICFS/payroll/check_date_20190301.xlsx'
    print(generate_signed_url(target_bucket_name, target_file_key))
