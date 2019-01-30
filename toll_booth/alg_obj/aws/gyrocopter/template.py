from toll_booth.alg_obj import AlgObject


class EmailTemplate(AlgObject):
    def __init__(self, template_name, subject_line, html_body, text_body):
        self._template_name = template_name
        self._subject_line = subject_line
        self._html_body = html_body
        self._text_body = text_body

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['template_name'], json_dict['subject_line'],
            json_dict['html_body'], json_dict.get('text_body', None)
        )

    @property
    def template_name(self):
        return self._template_name

    @property
    def subject_line(self):
        return self._subject_line

    @property
    def html_body(self):
        return self._html_body

    @property
    def text_body(self):
        return self._text_body

    @property
    def for_ses(self):
        return {
            'Template': {
                "TemplateName": self._template_name,
                "SubjectPart": self._subject_line,
                "HtmlPart": self._html_body,
                "TextPart": self._text_body
            }
        }
