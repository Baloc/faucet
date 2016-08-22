# -*- coding: utf8 -*-
# Copyright (C) 2015 Research and Education Advanced Network New Zealand Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time, os, random, json, io

import logging
from logging.handlers import TimedRotatingFileHandler


from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller import dpset
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from oslo_config import cfg

from influxdb import InfluxDBClient


def ship_points_to_influxdb(points, cfg):
    client = InfluxDBClient(
        host=cfg.influxdb_host, port=cfg.influxdb_port,
        username=cfg.influxdb_user, password=cfg.influxdb_pass,
        database=cfg.influxdb_db, timeout=10, ssl=cfg.influxdb_forcessl)
    return client.write_points(points=points, time_precision='s')


class GaugePortStateLogger(object):

    def __init__(self,ryudp, logname):
        self.ryudp = ryudp
        self.logger = logging.getLogger(logname)

    def update(self, rcv_time, msg):
        reason = msg.reason
        port_no = msg.desc.port_no
        ofp = msg.datapath.ofproto
        if reason == ofp.OFPPR_ADD:
            self.logger.info("port added %s", port_no)
        elif reason == ofp.OFPPR_DELETE:
            self.logger.info("port deleted %s", port_no)
        elif reason == ofp.OFPPR_MODIFY:
            link_down = (msg.desc.state & ofp.OFPPS_LINK_DOWN)
            if link_down:
                self.logger.info("port deleted %s", port_no)
            else:
                self.logger.info("port added %s", port_no)
        else:
            self.logger.info("Illegal port state %s %s", port_no, reason)


class GaugePortStateInfluxDBLogger(GaugePortStateLogger ):
    def __init__(self, cfg_influx):
        self.cfg_influx = cfg_influx
    def ship_points(self, points):
        return ship_points_to_influxdb(points, self.cfg_influx)

    def update(self, rcv_time, msg):
        super(GaugePortStateInfluxDBLogger, self).update(rcv_time, msg)
        reason = msg.reason
        port_no = msg.desc.port_no
        port_name = msg.desc.ryudp.id + "-PORT" + port_no
        port_tags = {
            "dp_name": msg.desc.ryudp.id,
            "port_name": port_name,
        }
        points = [{
            "measurement": "port_state_reason",
            "tags": port_tags,
            "time": int(rcv_time),
            "fields": {"value": reason}}]
        if not self.ship_points(points):
            self.logger.warning("error shipping port_state_reason points")


class GaugePoller(object):
    """A ryu thread object for sending and receiving openflow stats requests.

    The thread runs in a loop sending a request, sleeping then checking a
    response was received before sending another request.

    The methods send_req, update and no_response should be implemented by
    subclasses.
    """
    def __init__(self, ryudp, logname):
        self.ryudp = ryudp
        self.thread = None
        self.reply_pending = False
        self.logger = logging.getLogger(logname)
        # These values should be set by subclass
        self.interval = None
        self.logfile = None

    def start(self):
        self.stop()
        self.thread = hub.spawn(self)

    def stop(self):
        if self.thread is not None:
            hub.kill(self.thread)
            hub.joinall([self.thread])
            self.thread = None

    def __call__(self):
        """Send request loop.

        Delays the initial request for a random interval to reduce load.
        Then sends a request to the datapath, waits the specified interval and
        checks that a response has been received in a loop."""
        hub.sleep(random.randint(1, self.interval))
        while True:
            self.send_req()
            self.reply_pending = True
            hub.sleep(self.interval)
            if self.reply_pending:
                self.no_response(self.ryudp.id)

    def send_req(self):
        """Send a stats request to a datapath."""
        raise NotImplementedError

    def update(self, rcv_time, msg):
        """Handle the responses to requests.

        Called when a reply to a stats request sent by this object is received
        by the controller.

        It should acknowledge the receipt by setting self.reply_pending to
        false.

        Arguments:
        rcv_time -- the time the response was received
        msg -- the stats reply message
        """
        raise NotImplementedError

    def no_response(self, dp):
        """Called when a polling cycle passes without receiving a response."""
        raise NotImplementedError


class GaugeInfluxDBPoller(GaugePoller):
    def __init__(self, ryudb, logname,cfg_influxdb):
        super(GaugeInfluxDBPoller, self).__init__(ryudb, logname)
        self.cfg_influxdb = cfg_influxdb

    def ship_points(self, points):
        return ship_points_to_influxdb(points, self.cfg_influxdb)


class GaugePortStatsPoller(GaugePoller):
    """Periodically sends a port stats request to the datapath and parses and
    outputs the response."""
    def __init__(self, ryudp, logname):
        super(GaugePortStatsPoller, self).__init__(ryudp, logname)
        self.interval = 300
        self.logfile = None

    def send_req(self):
        ofp = self.ryudp.ofproto
        ofp_parser = self.ryudp.ofproto_parser
        req = ofp_parser.OFPPortStatsRequest(self.ryudp, 0, ofp.OFPP_ANY)
        self.ryudp.send_msg(req)

    def update(self, rcv_time, msg):
        # TODO: it may be worth while verifying this is the correct stats
        # response before doing this
        self.reply_pending = False
        rcv_time_str = time.strftime('%b %d %H:%M:%S')

        for stat in msg.body:
            if stat.port_no == msg.datapath.ofproto.OFPP_CONTROLLER:
                ref = msg.datapath.id + "-CONTROLLER"
            elif stat.port_no == msg.datapath.ofproto.OFPP_LOCAL:
                ref = msg.datapath.id + "-LOCAL"
            else:
                ref = msg.datapath.id + "-" + stat.port_no

            with open(self.logfile, 'a') as logfile:
                logfile.write('{0}\t{1}\t{2}\n'.format(rcv_time_str,
                                                       ref + "-packets-out",
                                                       stat.tx_packets))
                logfile.write('{0}\t{1}\t{2}\n'.format(rcv_time_str,
                                                       ref + "-packets-in",
                                                       stat.rx_packets))
                logfile.write('{0}\t{1}\t{2}\n'.format(rcv_time_str,
                                                       ref + "-bytes-out",
                                                       stat.tx_bytes))
                logfile.write('{0}\t{1}\t{2}\n'.format(rcv_time_str,
                                                       ref + "-bytes-in",
                                                       stat.rx_bytes))
                logfile.write('{0}\t{1}\t{2}\n'.format(rcv_time_str,
                                                       ref + "-dropped-out",
                                                       stat.tx_dropped))
                logfile.write('{0}\t{1}\t{2}\n'.format(rcv_time_str,
                                                       ref + "-dropped-in",
                                                       stat.rx_dropped))
                logfile.write('{0}\t{1}\t{2}\n'.format(rcv_time_str,
                                                       ref + "-errors-in",
                                                       stat.rx_errors))

    def no_response(self, dp):
        self.logger.info(
            "port stats request timed out for {0}".format(dp))


class GaugePortStatsInfluxDBPoller(GaugeInfluxDBPoller):
    """Periodically sends a port stats request to the datapath and parses and
    outputs the response."""
    def __init__(self, ryudp, logname, cfg_influx):
        super(GaugePortStatsInfluxDBPoller, self).__init__(ryudp, logname, cfg_influx)
        self.interval = 300

    def send_req(self):
        ofp = self.ryudp.ofproto
        ofp_parser = self.ryudp.ofproto_parser
        req = ofp_parser.OFPPortStatsRequest(self.ryudp, 0, ofp.OFPP_ANY)
        self.ryudp.send_msg(req)

    def update(self, rcv_time, msg):
        # TODO: it may be worth while verifying this is the correct stats
        # response before doing this
        self.reply_pending = False
        points = []

        for stat in msg.body:
            if stat.port_no == msg.datapath.ofproto.OFPP_CONTROLLER:
                port_name = "CONTROLLER"
            elif stat.port_no == msg.datapath.ofproto.OFPP_LOCAL:
                port_name = "LOCAL"
            else:
                port_name = stat.port_no

            port_tags = {
                "dp_name": msg.datapath.id,
                "port_name": port_name,
            }

            for stat_name, stat_value in (
                ("packets_out", stat.tx_packets),
                ("packets_in", stat.rx_packets),
                ("bytes_out", stat.tx_bytes),
                ("bytes_in", stat.rx_bytes),
                ("dropped_out", stat.tx_dropped),
                ("dropped_in", stat.rx_dropped),
                ("errors_in", stat.rx_errors)):
                points.append({
                    "measurement": stat_name,
                    "tags": port_tags,
                    "time": int(rcv_time),
                    "fields": {"value": stat_value}})
        if not self.ship_points(points):
            self.logger.warn("error shipping port_stats points")

    def no_response(self, dp):
        self.logger.info(
            "port stats request timed out for {0}".format(dp))


class GaugeFlowTablePoller(GaugePoller):
    """Periodically dumps the current datapath flow table as a yaml object.

    Includes a timestamp and a reference ($DATAPATHNAME-flowtables). The
    flow table is dumped as an OFFlowStatsReply message (in yaml format) that
    matches all flows."""
    def __init__(self, ryudp, logname):
        super(GaugeFlowTablePoller, self).__init__(ryudp, logname)
        self.interval = 300
        self.logfile = None

    def send_req(self):
        ofp = self.ryudp.ofproto
        ofp_parser = self.ryudp.ofproto_parser
        match = ofp_parser.OFPMatch()
        req = ofp_parser.OFPFlowStatsRequest(
            self.ryudp, 0, ofp.OFPTT_ALL, ofp.OFPP_ANY, ofp.OFPG_ANY,
            0, 0, match)
        self.ryudp.send_msg(req)

    def update(self, rcv_time, msg):
        # TODO: it may be worth while verifying this is the correct stats
        # response before doing this
        self.reply_pending = False
        jsondict = msg.to_jsondict()
        rcv_time_str = time.strftime('%b %d %H:%M:%S')

        with open(self.logfile, 'a') as logfile:
            ref = msg.datapath.id + "-flowtables"
            logfile.write("---\n")
            logfile.write("time: {0}\nref: {1}\nmsg: {2}\n".format(
                rcv_time_str, ref, json.dumps(jsondict, indent=4)))

    def no_response(self, dp):
        self.logger.info(
            "flow dump request timed out for {0}".format(dp))


class GaugeFlowTableInfuxDBPoller(GaugeInfluxDBPoller):

    def __init__(self, ryudb, logname, cfg_influxdb):
        super(GaugeFlowTableInfuxDBPoller, self).__init__(ryudb, logname, cfg_influxdb)
        self.interval = 300

    def decode_flow(self, stat):
        """
        Retrieve flow information form cookie.
        :param data: flow stat data output
        :param stat: flow stat send by the controller
        :return:
        """
        cookie = stat.cookie
        source = (cookie >> 32) - 1024
        if source < 0:
            return "IGNORE"
        destination = (cookie & 0x00000000FFFFFFFF) - 1024
        if destination < 0:
            return "IGNORE"
        type = self.guess_flow_type(stat)
        return source, destination, type

    def guess_flow_type(self, stat):
        """
        Guess flow type based on eth_type match component in the flow statistic.
        :param stat: flow statistic
        :return: flow_type
        """
        match = stat.match.oxm_fields
        if match.eth_type == 2054:
            return "ARP"
        elif match.eth_type == 2048:
            return "IPv4"
        elif match.eth_type == 34525:
            if "icmpv6_type" not in match:
                return "IPv6"
            else:
                return "ICMPv6"

    def send_req(self):
        ofp_parser = self.ryudp.ofproto_parser
        req = ofp_parser.OFPFlowStatsRequest(
            self.ryudp, 0, 1)
        self.ryudp.send_msg(req)

    def merge_two_dicts(self, x, y):
        '''Given two dicts, merge them into a new dict as a shallow copy.'''
        z = x.copy()
        z.update(y)
        return z

    def update(self, rcv_time, msg):
        """Handle the responses to requests.

        Called when a reply to a stats request sent by this object is received
        by the controller.

        It should acknowledge the receipt by setting self.reply_pending to
        false.

        Arguments:
        rcv_time -- the time the response was received
        msg -- the stats reply message
        """
        self.reply_pending = False
        points = []

        for stat in msg.body:
            # ignore tables other than 1
            if stat.table_id is not 1:
                continue
            decode = self.decode_flow(stat)

            flow_tags = {
                "dp_name": msg.datapath.id,
                "source": decode[0],
                "destination": decode[1],
                "type": decode[2]
            }

            for stat_name, stat_value in (
                    ("byte_count", stat.byte_count),
                    ("packet_count", stat.packet_count)):
                points.append({
                    "measurement": stat_name,
                    "tags": flow_tags,
                    "time": int(rcv_time),
                    "fields": {"value": stat_value}})
        if not self.ship_points(points):
            self.logger.warn("error shipping flow_table points")

    def no_response(self, dp):
        self.logger.info(
            "flow table stats request timed out for {0}".format(dp))


class Gauge(app_manager.RyuApp):
    """Ryu app for polling Faucet controlled datapaths for stats/state.

    It can poll multiple datapaths. The configuration files for each datapath
    should be listed, one per line, in the file set as the environment variable
    GAUGE_CONFIG. It logs to the file set as the environment variable
    GAUGE_LOG,
    """
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    _CONTEXTS = {'dpset': dpset.DPSet}

    logname = 'gauge'
    exc_logname = logname + '.exception'

    def load_config(self):
        # Test ouverture fichier
        try:
            self.CONF.register_group(cfg.OptGroup(name='gauge',
                                     title='gauge (faucet) controller options'))
            self.CONF.register_opts([
                                    cfg.StrOpt('influxdb_db'),
                                    cfg.StrOpt('influxdb_host'),
                                    cfg.IntOpt('influxdb_port'),
                                    cfg.StrOpt('influxdb_user'),
                                    cfg.StrOpt('influxdb_pass'),
                                    cfg.BoolOpt('influxdb_forcessl'),
                                    cfg.BoolOpt('enable')
                                    ], 'gauge')

            if self.CONF.gauge.enable is False:
                self.logger.warn("Application Contrôleur gauge désactivé")
                self.stop()
        except AttributeError:
            self.logger.error("Erreur : Chemin de fichier invalide")
            self.stop()
            return False
        except cfg.NoSuchOptError:
            self.logger.error("Erreur : Fichier de configuration invalide")
            self.stop()
            return False
        return True
    def __init__(self, *args, **kwargs):
        super(Gauge, self).__init__(*args, **kwargs)
        self.config_file = os.getenv(
            'GAUGE_CONFIG', '/etc/ryu/faucet/gauge.conf')
        self.exc_logfile = os.getenv(
            'GAUGE_EXCEPTION_LOG', '/var/log/ryu/faucet/gauge_exception.log')
        self.logfile = os.getenv('GAUGE_LOG', '/var/log/ryu/faucet/gauge.log')
        self.load_config()
        # Setup logging
        self.logger = logging.getLogger(__name__)
        logger_handler = TimedRotatingFileHandler(
            self.logfile,
            when='midnight')
        log_fmt = '%(asctime)s %(name)-6s %(levelname)-8s %(message)s'
        date_fmt = '%b %d %H:%M:%S'
        default_formatter = logging.Formatter(log_fmt, date_fmt)
        logger_handler.setFormatter(default_formatter)
        self.logger.addHandler(logger_handler)
        self.logger.propagate = 0

        # Set up separate logging for exceptions
        exc_logger = logging.getLogger(self.exc_logname)
        exc_logger_handler = logging.FileHandler(self.exc_logfile)
        exc_logger_handler.setFormatter(
            logging.Formatter(log_fmt, date_fmt))
        exc_logger.addHandler(exc_logger_handler)
        exc_logger.propagate = 1
        exc_logger.setLevel(logging.ERROR)


        # Create dpset object for querying Ryu's DPSet application
        self.dpset = kwargs['dpset']

        # dict of polling threads:
        # polling threads are indexed by dp_id and then by name
        # eg: self.pollers[0x1]['port_stats']
        self.pollers = {}
        # dict of async event handlers
        self.handlers = {}

    @set_ev_cls(dpset.EventDP, dpset.DPSET_EV_DISPATCHER)
    def handler_connect_or_disconnect(self, ev):
        ryudp = ev.dp


        if ev.enter: # DP is connecting
            self.logger.info("datapath up %x", ryudp.id)
            self.handler_datapath(ev)
        else: # DP is disconnecting
            if ryudp.id in self.pollers:
                for poller in self.pollers[ryudp.id].values():
                    poller.stop()
                del self.pollers[ryudp.id]
            self.logger.info("datapath down %x", ryudp.id)

    @set_ev_cls(dpset.EventDPReconnected, dpset.DPSET_EV_DISPATCHER)
    def handler_reconnect(self, ev):
        self.logger.info("datapath reconnected %x", ev.msg.datapath.id)
        self.handler_datapath(ev)

    def handler_datapath(self, ev):
        ryudp = ev.dp
        # Set up a thread to poll for port stats
        # TODO: set up threads to poll for other stats as well
        # TODO: allow the different things to be polled for to be
        # configurable
        if ryudp.id not in self.pollers:
            self.pollers[ryudp.id] = {}
            self.handlers[ryudp.id] = {}

        port_state_handler = GaugePortStateInfluxDBLogger(self.CONF.gauge)
        self.handlers[ryudp.id]['port_state'] = port_state_handler

        port_stats_poller = GaugePortStatsInfluxDBPoller(ryudp, self.logname, self.CONF.gauge)
        self.pollers[ryudp.id]['port_stats'] = port_stats_poller
        port_stats_poller.start()

        flow_table_poller = GaugeFlowTableInfuxDBPoller(ryudp, self.logname, self.CONF.gauge)
        self.pollers[ryudp.id]['flow_table'] = flow_table_poller
        flow_table_poller.start()

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER) # pylint: disable=no-member
    def port_status_handler(self, ev):
        rcv_time = time.time()
        self.handlers[ev.msg.datapath.id]['port_state'].update(rcv_time, ev.msg)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER) # pylint: disable=no-member
    def port_stats_reply_handler(self, ev):
        rcv_time = time.time()
        self.pollers[ev.msg.datapath.id]['port_stats'].update(rcv_time, ev.msg)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER) # pylint: disable=no-member
    def flow_stats_reply_handler(self, ev):
        rcv_time = time.time()
        self.pollers[ev.msg.datapath.id]['flow_table'].update(rcv_time, ev.msg)
