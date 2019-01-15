"""Converters for working with Qualtrics exports in Pandas"""

import json
import re
from collections import OrderedDict, defaultdict

import pandas as pd


def _blocks_to_df(qsf):
    survey_elements = [x for x in qsf['SurveyElements']
                       if x['Element'] == 'BL']
    if len(survey_elements) != 1:
        raise ValueError('Expected one BL element, got '
                         '{}'.format(len(survey_elements)))
    payload = survey_elements[0]['Payload']
    # XXX: sometimes the QSF contains a list, sometimes an object ...?
    if hasattr(payload, 'items'):
        blocks = payload.values()
    else:
        blocks = payload

    records = []
    for block in blocks:
        if 'BlockElements' not in block:
            continue
        block_rec = {'block_descr': block['Description'],
                     'block_id': block['ID'],
                     'block_type': block['Type']}
        for i, question in enumerate(block['BlockElements']):
            rec = block_rec.copy()
            rec['question_id'] = question['QuestionID']
            rec['question_type'] = question['Type']
            rec['question_idx'] = i
            records.append(rec)

    out = pd.DataFrame(records)
    return out


def _flows_to_df(qsf):
    survey_elements = [x for x in qsf['SurveyElements']
                       if x['Element'] == 'FL']
    if len(survey_elements) != 1:
        raise ValueError('Expected one FL element, got '
                         '{}'.format(len(survey_elements)))
    records = []
    for i, flow in enumerate(survey_elements[0]['Payload']['Flow']):
        records.append({'block_id': flow['ID'],
                        'flow_id': flow['FlowID'],
                        'flow_idx': i})
    return pd.DataFrame(records)


def _questions_to_df(qsf):
    """
    """
    records = []
    for element in qsf['SurveyElements']:
        if element['Element'] != 'SQ':
            continue

        question = element['Payload']
        rec = {'question_id': question['QuestionID'],
               'sub_question_id': question['QuestionID'],
               'export_id': question['DataExportTag'],
               'type': question['QuestionType'],
               'text': question['QuestionText']}
        if rec['type'] == 'MC':
            rec['choices'] = OrderedDict((str(k), question['Choices'][str(k)])
                                         for k in question['ChoiceOrder'])
            if question['Selector'] in ('MAVR', 'MSB'):
                rec['multi_valued'] = True
            elif question['Selector'] in ('SAVR', 'DL'):
                rec['multi_valued'] = False
            else:
                raise ValueError('Unknown selector: {}'.format(
                    question['Selector']))
            records.append(rec)
        elif rec['type'] == 'TE' and not question.get('Choices'):
            records.append(rec)
        elif rec['type'] == 'TE':
            # for TE type, choices represent multipart text entry
            for choice_key, choice in question['Choices'].items():
                subrec = rec.copy()
                subrec['sub_question_id'] += '_' + str(choice_key)
                subrec['text2'] = choice['Display']
                records.append(subrec)
        elif rec['type'] == 'DB':
            # Don't bother storing static text
            pass
        else:
            import pprint
            pprint.pprint(question)
            raise NotImplementedError(rec['type'])

    by_export_id = defaultdict(list)
    for rec in records:
        n = len(by_export_id[rec['export_id']])
        by_export_id[rec['export_id']].append(rec)
        if n:
            rec['export_id'] = rec['export_id'] + '.' + str(n)
    return pd.DataFrame(records)


def qsf_to_dataframe(qsf_data):
    """Extracts schema details from QSF as a pd.DataFrame

    Export to Qualtrics Survey Format is described at
    https://www.qualtrics.com/support/survey-platform/survey-module/survey-tools/import-and-export-surveys/

    qsf_data may be a path or file-like containing JSON data
    """
    if hasattr(qsf_data, 'lower'):
        with open(qsf_data) as f:
            return qsf_to_dataframe(f)
    elif hasattr(qsf_data, 'readlines'):
        qsf_data = json.load(qsf_data)
    df = pd.merge(_blocks_to_df(qsf_data), _flows_to_df(qsf_data),
                  on='block_id')
    df.sort_values(['flow_idx', 'question_idx'], inplace=True)
    df = pd.merge(df, _questions_to_df(qsf_data), how='inner')
    df.set_index('sub_question_id', inplace=True)
    return df


def load_and_enhance_response(schema, csv_path):
    """Enhances a qualtrics response CSV, incorporating aspects of the schema

    schema here is the output of qsf_to_dataframe, or is a QSF file
    """
    if not isinstance(schema, pd.DataFrame):
        schema = qsf_to_dataframe(schema)
    response = pd.read_csv(csv_path).transpose()
    response.insert(0, 'import_id', response[1].apply(
        lambda x: re.sub('_TEXT$', '', json.loads(x)['ImportId'])))
    response.insert(1, 'block', response.import_id.apply(
        lambda x: schema.block_descr.get(x, 'Meta')))
    response.rename(columns={0: 'question_text'}, inplace=True)
    response['question_text'] = response['question_text'].apply(
        lambda s: s.replace('\n', ' '))
    response.set_index('import_id', inplace=True)
    return response.transpose()
