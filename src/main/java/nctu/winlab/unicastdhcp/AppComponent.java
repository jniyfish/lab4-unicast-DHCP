/*
 * Copyright 2020-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nctu.winlab.unicastdhcp;

import com.google.common.collect.ImmutableSet;
import org.onosproject.cfg.ComponentConfigService;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.onlab.packet.Ethernet;
import org.onlab.packet.ICMP;
import org.onlab.packet.IPv4;
import org.onlab.packet.MacAddress;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.event.Event;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Host;
import org.onosproject.net.HostId;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.PortNumber;
import org.onosproject.net.host.HostService;
import org.onosproject.net.topology.TopologyService;
import org.onosproject.net.flow.DefaultFlowRule;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.TrafficTreatment;
import org.onosproject.net.flow.FlowEntry;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.flowobjective.DefaultForwardingObjective;
import org.onosproject.net.flowobjective.ForwardingObjective;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketPriority;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.packet.InboundPacket;
import java.util.Dictionary;
import java.util.Properties;
import static org.onlab.util.Tools.get;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.HashMap;
import org.onlab.packet.IPv4;
import org.onlab.packet.DHCP;
import org.onlab.packet.TpPort;
import org.onlab.packet.UDP;
import org.onosproject.net.config.ConfigFactory;
import org.onosproject.net.config.NetworkConfigEvent;
import org.onosproject.net.config.NetworkConfigListener;
import org.onosproject.net.config.NetworkConfigRegistry;
import org.onosproject.net.packet.DefaultOutboundPacket;
import java.util.Set;
import java.util.List;
import java.util.LinkedList;
import java.util.LinkedHashSet;
import org.onosproject.net.host.HostProvider;
import org.onosproject.net.host.HostProviderRegistry;
import org.onosproject.net.host.HostProviderService;

import static org.onosproject.net.config.NetworkConfigEvent.Type.CONFIG_ADDED;
import static org.onosproject.net.config.NetworkConfigEvent.Type.CONFIG_UPDATED;
import static org.onosproject.net.config.basics.SubjectFactories.APP_SUBJECT_FACTORY;



/**
 * Skeletal ONOS application component.
 */
@Component(immediate = true,
           service = {SomeInterface.class},
           property = {
               "someProperty=Some Default String Value",
           })
public class AppComponent implements SomeInterface {
    private int flowTimeout = 20;
    private int flowPriority = 40001;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final NameConfigListener cfgListener = new NameConfigListener();
    private final ConfigFactory factory =
    new ConfigFactory<ApplicationId, NameConfig>(
        APP_SUBJECT_FACTORY, NameConfig.class, "UnicastDhcpConfig") {
      @Override
      public NameConfig createConfig() {
        return new NameConfig();
      }
    };

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected NetworkConfigRegistry cfgService2;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected CoreService coreService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected ComponentConfigService cfgService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected PacketService packetService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected TopologyService topologyService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected FlowObjectiveService flowObjectiveService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected HostService hostService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected HostProviderRegistry hostProviderRegistry;

    protected HostProviderService hostProviderService;

    /** Some configurable property. */
    private String someProperty;
    private PacketProcessor processor ;
    private String server;
    private ConnectPoint serverCp;
    private MacAddress serverMac;
    private DeviceId serverId;
    private ApplicationId appId;
    private Map<MacAddress,ConnectPoint> macTables 
            = Maps.newConcurrentMap();
    private Set<Host> hostSet = new LinkedHashSet<Host>();;


    @Activate
    protected void activate() {
        appId = coreService.registerApplication("nctu.winlab.unicastdhcp");
        cfgService.registerProperties(getClass());
        cfgService2.addListener(cfgListener);
        cfgService2.registerConfigFactory(factory);
        processor = new ReactivePacketProcessor();
        packetService.addProcessor(processor, PacketProcessor.director(2));
        requestPackets();
        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        cfgService.unregisterProperties(getClass(), false);
        cfgService2.removeListener(cfgListener);
        cfgService2.unregisterConfigFactory(factory);
        log.info("Stopped");
    }

    @Modified
    public void modified(ComponentContext context) {
        Dictionary<?, ?> properties = context != null ? context.getProperties() : new Properties();
        if (context != null) {
            someProperty = get(properties, "someProperty");
        }
        log.info("Reconfigured");
    }

    @Override
    public void someMethod() {
        log.info("Invoked");
    }
    private class NameConfigListener implements NetworkConfigListener {
        @Override
        public void event(NetworkConfigEvent event) {
          if ((event.type() == CONFIG_ADDED || event.type() == CONFIG_UPDATED)
              && event.configClass().equals(NameConfig.class)) {
            NameConfig config = cfgService2.getConfig(appId, NameConfig.class);
            if (config != null) {
              log.info("DHCP server is at {}", config.name());
              server = config.name();
              serverCp = serverPoint(server);
              serverId = serverCp.deviceId();

            }
          }
        }
      }
    private void requestPackets() {

        TrafficSelector.Builder selectorServer = DefaultTrafficSelector.builder()
                .matchEthType(Ethernet.TYPE_IPV4)
                .matchIPProtocol(IPv4.PROTOCOL_UDP);
        packetService.requestPackets(selectorServer.build(), PacketPriority.CONTROL, appId);

        selectorServer = DefaultTrafficSelector.builder()
                .matchEthType(Ethernet.TYPE_ARP);
        packetService.requestPackets(selectorServer.build(), PacketPriority.CONTROL, appId);

        selectorServer = DefaultTrafficSelector.builder()
        .matchEthType(Ethernet.TYPE_IPV4);
        packetService.requestPackets(selectorServer.build(), PacketPriority.CONTROL, appId);
    }
    private class ReactivePacketProcessor implements PacketProcessor {

        @Override
        public void process(PacketContext context) {

                if (context.isHandled()) {
                    return;
                }   
                InboundPacket pkt = context.inPacket();
                Ethernet ethPkt = pkt.parsed();
                if (ethPkt == null) {
                    return;
                }
                ConnectPoint lastPort= pkt.receivedFrom();

                if(ethPkt.getDestinationMAC().toString().equals("FF:FF:FF:FF:FF:FF") ){
                    Path path = calculatePath(context,1);
                    List<Link> linkSet = path.links();
                    //packetOut(context, path.src().port());
                    for(int i=0;i<linkSet.size();i++){
                        Link l = linkSet.get(i);
                        installRule(l.src(),lastPort);
                        installRule(lastPort,l.src());
                        lastPort = l.dst();
                    }
                    installRule(serverCp,lastPort);
                    installRule(lastPort,serverCp);
                }

            }
    }
    private void packetOut(PacketContext context, PortNumber portNumber) {
        context.treatmentBuilder().setOutput(portNumber);
        context.send();
    }
    private Path calculatePath(PacketContext context,int flag) {
        InboundPacket inPkt = context.inPacket();
        Ethernet ethPkt = inPkt.parsed();

        DeviceId dstId;

        if(flag==1){
            dstId = serverId;
        }
        Set<Path> paths = topologyService.getPaths(topologyService.currentTopology(), inPkt.receivedFrom().deviceId(),
                serverId);
        if (paths.isEmpty()) {
            return null;
        }
        Path path = pickForwardPathIfPossible(paths, inPkt.receivedFrom().port());
        if (path == null) {
            log.info("No path");
            return null;
        } else {
            return path;
        }
    }
    private void installRule(ConnectPoint src,ConnectPoint lastPort) {
        TrafficSelector.Builder selectorBuilder = DefaultTrafficSelector.builder();
        selectorBuilder.matchInPort(lastPort.port());
        selectorBuilder.matchEthType(Ethernet.TYPE_IPV4);

        TrafficTreatment.Builder treatmentBuilder = DefaultTrafficTreatment.builder();
        treatmentBuilder.setOutput(src.port());

        ForwardingObjective forwardingObjective = DefaultForwardingObjective.builder()
                .withSelector(selectorBuilder.build())
                .withTreatment(treatmentBuilder.build())
                .withPriority(flowPriority)
                .withFlag(ForwardingObjective.Flag.VERSATILE)
                .fromApp(appId)
                .makeTemporary(flowTimeout)
                .add();

        flowObjectiveService.forward(src.deviceId(), forwardingObjective);

    }

    private Path pickForwardPathIfPossible(Set<Path> paths, PortNumber notToPort) {
        Path lastPath = null;
        for (Path path : paths) {
            lastPath = path;
            if (!path.src().port().equals(notToPort)) {
                return path;
            }
        }
        return lastPath;
    }
    private static ConnectPoint serverPoint(String string) {
        int idx = string.lastIndexOf("/");
        String id = string.substring(0, idx);
        String cp = string.substring(idx + 1);
        return new ConnectPoint(DeviceId.deviceId(id), PortNumber.portNumber(cp));
    }

}
