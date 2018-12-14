ROW_PATTERN = '(<table border=\"\d\"\scellpadding=\"\d\"\scellspacing=\"\d\"\swidth=\"[\d]+%\">)(?P<rows>[.\s\S]+?)(</table>)'
NAME_PATTERN = '(?P<last_name>\w+),\s+(?P<first_initial>\w)'
