#!/usr/bin/env python

import logging
import configargparse
import datetime
import time
import json
import getpass
import traceback
import sys
import copy
import re
import pandas as pd
from logging.config import dictConfig
from configargparse import YAMLConfigFileParser
from Exscript.protocols import SSH2
from Exscript.protocols.Exception import LoginFailure
from Exscript.protocols.Exception import InvalidCommandException
from Exscript import Account
from ciscomation.ciscomation_mp import mp_manager
from ciscomation.ciscomation_exc import CiscomationLoginFailed
from ciscomation.ciscomation_exc import CiscomationException
from ciscomation.ciscomation_xml import xml_to_maintenance

__SCRIPT__ = 'ciscomation'


def exc_txt(sys_exc_info):
    '''
    Returns a text view of sys.exc.info data.
    '''
    exc_type, exc_value, exc_traceback = sys_exc_info
    etxt = traceback.format_exception(exc_type, exc_value, exc_traceback)
    return '\n'.join(etxt)


def pause():
    '''
    Prompts you for going further.
    '''
    txtinput = raw_input("continue [y/N]? ")
    if txtinput.lower()[0] != 'y':
        print "Stopping it all here."
        exit(1)


def set_connection(host, login, password, driver='ios'):
    '''
    set_connection configures Exscript SSH2 Connection and validate the device
    type.

    Parameters
    ----------
    host : str
        device to connect to

    login: str
        login credential

    password: str
        password credential

    driver : str, optional
        base driver to test.

    Returns
    -------
    connection: Exscript.protocols.SSH2
        SSH2 object usable with proper driver.
    '''
    logs = []
    LOGGER = logging.getLogger(__SCRIPT__)
    connection = SSH2(driver=driver, debug=0, verify_fingerprint=False,
                      connect_timeout=7, timeout=100, termtype='vt100')
    connection.connect(str(host).strip())
    account = Account(login, password)
    for attempt in range(4):
        try:
            connection.authenticate(account)
            break
        except LoginFailure:
            time.sleep(0.5 + (float(attempt) / 2))
            LOGGER.error(
                'Login attempt number %d failed for host %s.',
                attempt + 1,
                host
            )
            if attempt == 3:
                raise CiscomationLoginFailed(
                    (
                        '4 Login Failure be careful your login'
                        ' could be locked'
                    )
                )
    logs.append(('info', 'Login on switch {}'.format(str(host))))
    connection.autoinit()
    specific_version = None
    try:
        connection.execute('show version')
        if ' IOS ' in connection.response:
            connection.set_driver('ios')
        elif ' (NX-OS) ' in connection.response:
            connection.set_driver('nxos')
            connection.get_error_prompt().append(
                re.compile(r'^% invalid command', re.I)
            )
            connection.get_error_prompt().append(
                re.compile(r'^% invalid parameter', re.I)
            )
        elif ' IOS-XE ' in connection.response:
            connection.set_driver('ios')
    except:
        connection.set_driver('ios')
    logs.append(
        (
            'info', 'Using driver {} for host {}'.format(
                str(host), connection.get_driver().name
            )
        )
    )
    return (connection, specific_version, logs)


def run_commands(host, login, password, driver=None, commands=["show version"],
                 abort_on_error=True, conf_mode=False, save=False,
                 continue_on_login_failure=True, pause_end=False):
    '''
    run_commands, run a list of commands

    Parameters
    ----------
    host : str
        device to connect to

    login: str
        login credential

    password: str
        password credential

    driver : str, optional
        Set it only if you are sure of the driver you want to use.

    abort_on_error: bool
        Defaults to True, if True will stop if a command failes

    conf_mode: bool
        Defaults to False, if True the configuration mode will be enabled
        before running the commands

    conf_mode: bool
        Defaults to False, if True the configuration mode will be enabled

    Returns
    -------
    result: dict
        dict image of the retrun from the commands. Structure is like so

        .. code-block:: python

            result = {
                host: {
                    'driver' : 'ios',
                    'status_ok': True,
                    'commands': [
                        {
                            "configure terminal": "Enter configuration..."
                        },
                        {
                        "no ip access-list standard SNMP_RO": ""
                        }
                    ],
                    'logs': []
                }
            }

    '''
    result = {
        host: {
            'driver': 'default',
            'status_ok': True,
            'all_commands_ok': True,
            'commands': [],
            'logs': []
        }
    }
    state = {
        'print-next': False,
        'multiline': False,
        'ignore-error': False,
        'multilines': []
    }
    # %% Setting up connection
    try:
        connection, specific_version, conlogs = set_connection(
            host, login, password, driver='ios'
        )
        result[host]['logs'].extend(conlogs)
        if specific_version:
            result[host]['driver'] = specific_version
        else:
            result[host]['driver'] = connection.get_driver().name
    except CiscomationLoginFailed as exc:
        result[host]['status_ok'] = False
        result[host]['logs'].append(
            (
                'critical',
                '{} Connection Failed : {}'.format(host, str(exc))
            )
        )
        if not continue_on_login_failure:
            raise exc
        else:
            return result
    except Exception as exc:
        result[host]['status_ok'] = False
        result[host]['logs'].append(
            (
                'critical',
                '{} Connection Failed : {}'.format(host, str(exc))
            )
        )
        result[host]['logs'].append(
            (
                'debug',
                '{} details:\n{}'.format(host, exc_txt(sys.exc_info()))
            )
        )
        return result
    # %% enforcing driver if specified if needed adding conf mode and saving
    if driver:
        connection.set_driver(driver)
        result[host]['driver'] = driver
        result[host]['logs'].append(
            (
                'debug',
                '{} Driver Set Manually to {} was found {}'.format(
                    host,
                    driver,
                    result[host]['driver']
                )
            )
        )
    if result[host]['driver'] == 'ios':
        if conf_mode is True:
            commands.insert(0, 'configure terminal')
            commands.append('end')
        if save is True:
            commands.append('wr mem')
    elif result[host]['driver'] == 'nxos':
        if conf_mode is True:
            commands.insert(0, 'configure terminal')
            commands.append('end')
        if save is True:
            commands.append('copy running startup')
        if save is True:
            commands.append('copy running startup')
    else:
        result[host]['logs'].append(
            ('error', '{} Unknown driver.'.format(host))
        )
        return result
    # %% Executing commands
    for command in commands:
        command = command.strip('\n\r')
        keyword = command.strip()
        ######################################################################
        # detecting special keywords
        if keyword == '--multiline-stop':
            state['multiline'] = False
            result[host]['logs'].append(
                (
                    'debug',
                    '{} Leaving multiline'.format(host)
                )
            )
            try:
                connection.execute('')
                resp = connection.response.replace('\r', '').split('\n')[1:-1]
                result[host]['commands'].append(
                    {
                        ' :: '.join(state['multilines']): '\n'.join(resp)
                    }
                )
            except InvalidCommandException as cmdex:
                result[host]['all_commands_ok'] = False
                result[host]['commands'].append(
                    {
                        command: None
                    }
                )
                result[host]['logs'].append(
                    (
                        'error',
                        '{} Command {} Failed with error : {}'.format(
                            host,
                            command,
                            str(cmdex)
                        )
                    )
                )
                if abort_on_error:
                    return result
            continue
        elif keyword.startswith('--sleep-'):
            timer = keyword.replace('--sleep-', '')
            try:
                timer = int(timer)
            except ValueError:
                result[host]['logs'].append(
                    (
                        'error',
                        (
                            '{} Wrong timer value {} I will pause for 5 '
                            'seconds.'
                        ).format(host, timer)
                    )
                )
                timer = 5
            result[host]['logs'].append(
                (
                    'info',
                    '{} sleeping for {} seconds'.format(host, timer)
                )
            )
            time.sleep(timer)
            continue
        elif keyword == '--multiline-start':
            state['multiline'] = True
            state['multilines'] = []
            result[host]['logs'].append(
                (
                    'debug',
                    'Entering multiline'
                )
            )
            continue
        elif keyword == '--pause':
            pause()
            continue
        elif keyword == '--ignore-error':
            state['ignore-error'] = True
            result[host]['logs'].append(
                (
                    'debug',
                    '{} Ignoring next line potential error'.format(host)
                )
            )
            continue
        elif keyword == '--print-next':
            state['print-next'] = True
            continue
        elif keyword.startswith('--'):
            result[host]['logs'].append(
                (
                    'error',
                    (
                        '{} This function does not seem to be implemented in'
                        ' current vesion, sorry I will not apply: {}.'
                    ).format(host, keyword)
                )
            )
            continue
        ######################################################################
        # really executing the commands
        try:
            if state['multiline']:
                state['multilines'].append(command + '\n')
                connection.send(command + '\n')
                continue
            else:
                connection.execute(command)
                resp = connection.response.replace('\r', '').split('\n')[1:-1]
                result[host]['commands'].append(
                    {
                        command: '\n'.join(resp)
                    }
                )
                if state['print-next']:
                    print(
                        '{} retuned:\n    {}'.format(
                            command,
                            '\n    '.join(resp)
                        )
                    )
        except InvalidCommandException as cmdex:
            result[host]['all_commands_ok'] = False
            result[host]['commands'].append(
                {
                    command: None
                }
            )
            result[host]['logs'].append(
                (
                    'error',
                    '{} Command {} Failed with error : {}'.format(
                        host,
                        command,
                        str(cmdex)
                    )
                )
            )
            if abort_on_error and not state['ignore-error']:
                return result
        state.update(
            {
                'print-next': False,
                'ignore-error': False
            }
        )
    if pause_end:
        pause()
    return result


def logconfig(args):
    '''
    Function to create a global LOGGER for the module.

    Parameters
    ----------
    args : argparse.Namespace
        Arguments passed to the script should contain args.xml_file,
        args.log_dir, args.log_level
    '''
    logfile = args.xml_file.replace('\\', '/').split('/')[-1]
    date = datetime.datetime.now()
    logfile = date.strftime("%y%m%d_%H%M%S_") + logfile + '.log'
    logging_config = {
        'version': 1,
        'formatters': {
            'basic_f': {
                'format': (
                    '%(asctime)s %(name)-'
                    '12s %(levelname)-8s %(funcName)-15s %(lineno)-4s '
                    '%(message)s'
                )
            }
        },
        'handlers': {
            'logfile': {
                'class': 'logging.FileHandler',
                'formatter': 'basic_f',
                'level': 'DEBUG',
                'filename': '{}'.format(args.log_dir + '/' + logfile),
                'mode': 'w'
            }
        },
        'loggers': {
            '': {
                'handlers': ['logfile'],
                'level': '{}'.format(args.log_level.upper()),
                'propagate': True
            },
            'paramiko': {
                'handlers': ['logfile'],
                'level': '{}'.format('error'.upper()),
                'propagate': False
            },
            __SCRIPT__: {
                'handlers': ['logfile'],
                'level': '{}'.format(args.log_level.upper()),
                'propagate': False
            }
        }
    }
    dictConfig(logging_config)
    LOGGER = logging.getLogger(__SCRIPT__)
    LOGGER.debug('Log file opened')


def run_maint(maint_data, credentials, procnum=1):
    '''
    Execute a maintenance, uing specified credentials for SSH access, and
    attempts to run it with multiprocess, if maintenance as been recognized
    compatible.

    Parameters
    ----------
    maint_data : dict
        maintenance file detail like so :
    '''
    results = []
    LOGGER = logging.getLogger(__SCRIPT__)
    if procnum == 1 or not maint_data['mp_compat']:
        for switch in maint_data['actions']:
            data = run_commands(
                switch['swname'],
                credentials[0],
                credentials[1],
                commands=switch['commands'],
                abort_on_error=True,
                conf_mode=False,
                save=False,
                continue_on_login_failure=True,
                pause_end=switch['pause']
            )
            results.append(data)
            if 'logs' in data[data.keys()[0]]:
                for log in data[data.keys()[0]]['logs']:
                    LOGGER.log(logging.getLevelName(log[0].upper()), log[1])
    elif procnum > 1 and maint_data['mp_compat']:
        args_list = []
        for switch in maint_data['actions']:
            args_list.append(
                {
                    'args': [
                        switch['swname'],
                        credentials[0],
                        credentials[1]
                    ],
                    'kwargs': {
                        'commands': switch['commands'],
                        'abort_on_error': True,
                        'conf_mode': False,
                        'save': False,
                        'continue_on_login_failure': True,
                        'pause_end': switch['pause']
                    }
                }
            )
        func = run_commands
        results = mp_manager(func, args_list, threads_count=procnum)
    else:
        raise CiscomationException('procum parameter cannot be null')
    dict_result = {}
    for data in results:
        dict_result.update(data)
    return dict_result


def arg_conf(description):
    '''
    Reading argument given to the script.
    '''
    parser = configargparse.ArgParser(
        default_config_files=[
            '/etc/%s.yml' % __SCRIPT__,
            '~/%s.yml' % __SCRIPT__,
            './%s.yml' % __SCRIPT__
        ],
        description=description,
        config_file_parser_class=YAMLConfigFileParser
    )
    parser.add(
        '-i', '--xml-file',
        dest='xml_file',
        type=str,
        required=True,
        help='Name of the xml file containing maintenance'
    )
    parser.add(
        '--log-level',
        type=str,
        dest='log_level',
        default='error',
        help='Choose log level in debug, info, warning, error, critical'
    )
    parser.add(
        '--log-dir',
        type=str,
        dest='log_dir',
        default='./log',
        help='Path of the directory to put the logfiles'
    )
    parser.add(
        '--procnum',
        type=int,
        dest='procnum',
        default=1,
        help=(
            'Number of process if maintenance is compatible with multi '
            'process.'
        )
    )
    return parser.parse_args()
    #######################################################


def main():
    '''
    Main task
    '''
    ARGS = arg_conf(
        'This script takes an XML input file'
        '  reads the switches from it and plays the commands'
        ' specified'
    )
    CREDENTIALS = (
        raw_input('Username: '),
        getpass.getpass()
    )
    logconfig(ARGS)
    DATE = datetime.datetime.now()
    MAINT = xml_to_maintenance(ARGS.xml_file)
    with open('./maintenance.txt', 'wb') as dumpfile:
        json.dump(MAINT, dumpfile, indent=4)
    RESULTS = run_maint(MAINT, CREDENTIALS, procnum=int(ARGS.procnum))
    DUMPFILE = 'dump_{}.txt'.format(DATE.strftime("%y%m%d_%H%M%S"))
    CMDFILE = 'cmd_{}.txt'.format(DATE.strftime("%y%m%d_%H%M%S"))
    XLSXFILE = '{}_{}.xlsx'.format(
        ARGS.xml_file.replace('\\', '/').split('/')[-1],
        DATE.strftime("%y%m%d_%H%M%S"),
    )
    with open(DUMPFILE, 'wb') as dumpfile:
        json.dump(RESULTS, dumpfile, indent=4)
    # writing report
    RESULTS_COPY = copy.deepcopy(RESULTS)
    for hostname, feedback in RESULTS_COPY.items():
        feedback['commands'] = len(feedback['commands'])
        maintlogs = {
            'debug': 0,
            'info': 0,
            'warning': 0,
            'error': 0,
            'critical': 0
        }
        for thislog in feedback['logs']:
            maintlogs[thislog[0]] += 1
        feedback.update(maintlogs)
        del feedback['logs']
    TABLE_REPORT = pd.DataFrame(RESULTS_COPY).T
    TABLE_REPORT = TABLE_REPORT.rename(
        columns={
            'debug': 'log_debug',
            'info': 'log_info',
            'warning': 'log_warning',
            'error': 'log_error',
            'critical': 'log_critical'
        }
    )
    TABLE_REPORT[sorted(list(TABLE_REPORT.columns))].to_excel(XLSXFILE)
    with open(CMDFILE, 'wb') as cmdresult:
        for hostname, feedback in RESULTS.items():
            cmdresult.write('Host : {}\n'.format(hostname))
            indent = '    '
            for command in feedback['commands']:
                cmdresult.write(
                    '{}CMD: {}\n'.format(
                        indent,
                        command.keys()[0]
                    )
                )
                for line in command[command.keys()[0]].splitlines():
                    cmdresult.write(
                        '{}{}\n'.format(
                            indent*2,
                            line
                        )
                    )


if __name__ == '__main__':
    main()
