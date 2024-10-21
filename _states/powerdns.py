__virtualname__ = 'powerdns'

def __virtual__():
    if 'powerdns.get_zone' in __salt__:
        return __virtualname__
    return False

import logging
log = logging.getLogger(__name__)

def zone_present(name, kind=None, rrsets=None, masters=None, dnssec=None, nsec3param=None, nsec3narrow=None, presigned=None, soa_edit=None, soa_edit_api=None, api_rectify=None, catalog=None, nameservers=None, master_tsig_key_ids=None, slave_tsig_key_ids=None):

  want_data = {
    key: value
    for key, value in locals().items()
    if value is not None
  }

  zone = __salt__['powerdns.canonicalize_name'](name)
  want_data['name'] = zone

  if 'kind' in want_data:
    want_data['kind'] = want_data['kind'].capitalize()

  ret = {'name': zone, 'changes': {}, 'result': False, 'comment': ''}

  session = __salt__['powerdns.new_session'](True)
  log.debug('powerdns: got session')
  
  exists = __salt__['powerdns.get_zone_exists'](zone, session)

  log.debug(f'powerdns: zone exists => {exists}')
  log.debug(f'powerdns: want data => {want_data}')

  if exists:
    if 'nameservers' in want_data:
      del want_data['nameservers']

    have_data = {
      key: value
      for key, value in __salt__['powerdns.get_zone'](zone, session).items()
      if key in want_data
    }

    log.debug(f'powerdns: have data => {have_data}')

    payload = {}
    for key, want_value in want_data.items():
      log.debug(f'powerdns: reading wanted key "{key}"')
      have_value = have_data[key]
      if want_value != have_value:
        log.debug('powerdns: key differs')
        ret['changes'][key] = {
          'old': have_value,
          'new': want_value,
        }

        payload.update(
          {
            key: want_value,
          }
        )

    if not ret['changes']:
      ret['result'] = True
      ret['comment'] = 'Zone is already in the correct state.'
      return ret

    if ret['changes']:
      if __opts__['test']:
        ret['result'] = None
        ret['comment'] = 'Zone would be modified.'
        return ret

    if not 'rrsets' in payload:
      payload['rrsets'] = []

    ok, status, output = __salt__['powerdns.patch_zone'](zone, payload, 'REPLACE', session)

    if ok:
      ret['result'] = True
      ret['comment'] = 'Zone modified: {status} - {output}'

    else:
      ret['result'] = False
      ret['comment'] = f'Zone modification failed: {status} - {output}'

    return ret

  else:  # zone does not exist
    ret['changes'] = {
      'new': want_data,
      'old': {},
    }

    if __opts__['test']:
      ret['result'] = None
      ret['comment'] = 'Zone would be created.'
      return ret

    payload = want_data
    ok, status, output = __salt__['powerdns.post_zone'](zone, payload, session)

    if ok:
      ret['result'] = True
      ret['comment'] = f'Zone created: {status} - {output}'

    else:
      ret['result'] = False
      ret['comment'] = f'Zone creation failed: {status} - {output}'

    return ret

def rrsets_present(name, rrsets):
  zone = __salt__['powerdns.canonicalize_name'](name)
  ret = {'name': zone, 'changes': {}, 'result': False, 'comment': ''}

  session = __salt__['powerdns.new_session'](True)
  log.debug('powerdns: got session')

  have_rrsets = __salt__['powerdns.get_zone_rrsets'](zone, session)

  want_rrsets = []
  payload = []

  for rrset in rrsets:
    this_rrset = {
      'name': __salt__['powerdns.canonicalize_recname'](zone, rrset['name']),
      'type': rrset['type'],
    }

    if 'ttl' in rrset:
      this_rrset['ttl'] = rrset['ttl']

    this_rrset['records'] = [
      {
        'content': record,
        'disabled': False,
      }
      for record in rrset['records']
    ]

    want_rrsets.append(this_rrset)

  up_to_date_rrsets = []
  only_ttl_rrsets = []
  only_records_rrsets = []

  log.debug('powerdns: rrset payload BEFORE mangling')
  log.debug(want_rrsets)

  # TODO: this needs a second iteration over have_rrsets to remove records which are no longer in Salt

  for i, want_rrset in enumerate(want_rrsets):
    log.debug(want_rrset)
    for have_rrset in have_rrsets:
      log.debug(have_rrset)
      if want_rrset['name'] == have_rrset['name'] and have_rrset['type'] == want_rrset['type']:
        ttl_ok = False
        records_ok = []

        if 'ttl' in want_rrset and want_rrset['ttl'] == have_rrset['ttl'] or not 'ttl' in want_rrset:
          ttl_ok = True

        for ir, want_record in enumerate(want_rrset['records']):
          log.debug(f'powerdns: reading wanted record {want_record}')

          for have_record in have_rrset['records']:
            log.debug(f'powernds: comparing against {have_record}')

            if have_record == want_record:
              log.debug('powerdns: matches')
              records_ok.append(ir)
              break

            elif have_record['content'] == want_record['content'] and have_record['disabled'] != want_record['disabled']:
              log.debug('powerdns: status mismatch')

              if not want_rrset['name'] in ret['changes']:
                ret['changes'][want_rrset['name']] = [{}]

              if not 'old' in ret['changes'][want_rrset['name']][i] and not 'new' in ret['changes'][want_rrset['name']][i]:
                ret['changes'][want_rrset['name']][i]['old'] = {}
                ret['changes'][want_rrset['name']][i]['new'] = {}
                
              ret['changes'][want_rrset['name']][i]['old'].update({have_record['content']: not have_record['disabled']})
              ret['changes'][want_rrset['name']][i]['new'].update({want_record['content']: not want_record['disabled']})

              break

          else:
            log.debug(f'powerdns: not found')

            if not want_rrset['name'] in ret['changes']:
              ret['changes'][want_rrset['name']] = [{}]

            if not 'old' in ret['changes'][want_rrset['name']][i] and not 'new' in ret['changes'][want_rrset['name']][i]:
              ret['changes'][want_rrset['name']][i]['old'] = {}
              ret['changes'][want_rrset['name']][i]['new'] = {}

            ret['changes'][want_rrset['name']][i]['old'].update({want_record['content']: None})
            ret['changes'][want_rrset['name']][i]['new'].update({want_record['content']: not want_record['disabled']})

        all_records_ok = len(records_ok) == len(want_rrset['records'])
        if ttl_ok and all_records_ok:
          up_to_date_rrsets.append(i)

        elif not ttl_ok and all_records_ok:
          only_ttl_rrsets.append(i)
          if not want_rrset['name'] in ret['changes']:
            ret['changes'][want_rrset['name']] = [{}]
          ret['changes'][want_rrset['name']][i] = {
            'old': {'ttl': have_rrset['ttl']},
            'new': {'ttl': want_rrset['ttl']},
          }

        elif ttl_ok and not all_records_ok:
          only_records_rrsets.append(i)

          # update of individual records not possible / this might be ok to drop ; just always write all records in the set
          #for ir in records_ok:
          #  want_rrset['records'].pop(ir)

        break

    else:
      ret['changes'][want_rrset['name']] = [
        {
          'old': {},
          'new': {
            'ttl': want_rrset['ttl'],
            'records': {
              record['content']: not record['disabled']
              for record in want_rrset.get('records', [])
            }
          },
        },
      ]

  for i in up_to_date_rrsets:
    want_rrsets.pop(i-1)

  # API throws "No change for RRset" if attempting to patch only the TTL without records, bug?
  #for i in only_ttl_rrsets:
  #  del want_rrsets[i]['records']

  # API requires ttl for record updates, this might be ok to drop
  #for i in only_records_rrsets:
  #  if 'ttl' in want_rrsets[i]:
  #    del want_rrsets[i]['ttl']

  log.debug('powerdns: rrset changes')
  log.debug(ret['changes'])
  log.debug('powerdns: rrset payload AFTER mangling')
  log.debug(want_rrsets)

  if want_rrsets:
    if __opts__['test']:
      ret['result'] = None
      ret['comment'] = 'Resource sets would be updated.'
      return ret

    ok, status, output = __salt__['powerdns.patch_rrsets'](zone, 'REPLACE', session, rrsets=want_rrsets)

    if ok:
      ret['result'] = True
      ret['comment'] = f'Resource sets updated: {status} - {output}'

    else:
      ret['result'] = False
      ret['comment'] = f'Resource set update failed: {status} - {output}'

    return ret

  ret['result'] = True
  ret['comment'] = 'Resource sets are already up to date.'
  return ret
