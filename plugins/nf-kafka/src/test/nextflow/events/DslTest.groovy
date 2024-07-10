/*
 * Copyright 2020-2022, Seqera Labs
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
 *
 */

package nextflow.events


import nextflow.plugin.Plugins
import nextflow.plugin.TestPluginDescriptorFinder
import nextflow.plugin.TestPluginManager
import nextflow.plugin.extension.PluginExtensionProvider
import org.pf4j.PluginDescriptorFinder
import spock.lang.Shared
import spock.lang.Timeout
import test.Dsl2Spec
import test.KafkaContainerTrait
import test.MockScriptRunner

import java.nio.file.Path

/**
 *
 * @author Jorge Aguilera <jorge.aguilera@seqera.io>
 */
@Timeout(10)
class DslTest extends Dsl2Spec implements KafkaContainerTrait{

    @Shared String pluginsMode

    def setup() {
        // reset previous instances
        PluginExtensionProvider.reset()
        // this need to be set *before* the plugin manager class is created
        pluginsMode = System.getProperty('pf4j.mode')
        System.setProperty('pf4j.mode', 'dev')
        // the plugin root should
        def root = Path.of('.').toAbsolutePath().normalize()
        def manager = new TestPluginManager(root){
            @Override
            protected PluginDescriptorFinder createPluginDescriptorFinder() {
                return new TestPluginDescriptorFinder(){
                    @Override
                    protected Path getManifestPath(Path pluginPath) {
                        return pluginPath.resolve('build/resources/main/META-INF/MANIFEST.MF')
                    }
                }
            }
        }
        Plugins.init(root, 'dev', manager)

        startKafka()
    }

    def cleanup() {
        Plugins.stop()
        PluginExtensionProvider.reset()
        pluginsMode ? System.setProperty('pf4j.mode',pluginsMode) : System.clearProperty('pf4j.mode')

        stopKafka()
    }

    def 'should read from a topic' () {
        given:
        def config = [
                kafka : [
                    url:kafkaURL(),
                    group:'group'
                ]
        ]

        writeKafkaMessage("test", "Hola")
        sleep 500
        writeKafkaMessage("test", "Hi", "key")

        when:
        def SCRIPT = '''
            include { fromTopic } from 'plugin/nf-kafka'
            
            channel.fromTopic("test") 
            '''
        and:
        def result = new MockScriptRunner(config).setScript(SCRIPT).execute()

        then:
        [result.val.value,result.val.value].sort() == ['Hola','Hi'].sort()
    }

    // def 'should listen from a topic' () {
    //     given:
    //     def config = [
    //             kafka : [
    //                     url:kafkaURL(),
    //                     group:'group'
    //             ]
    //     ]

    //     writeKafkaMessage("test", "Hola")
    //     writeKafkaMessage("test", "Hi")
    //     writeKafkaMessage("test", "end", null)
    //     when:
    //     def SCRIPT = '''
    //         include { watchTopic } from 'plugin/nf-kafka'
                        
    //         chn = channel.watchTopic("test", [key:null, value:"end"])
            
    //         '''
    //     and:
    //     def result = new MockScriptRunner(config).setScript(SCRIPT).execute()

    //     then:
    //     [result.val[1],result.val[1]].sort() == ['Hola','Hi'].sort()
    // }

    // def 'should send messages to a process' () {
    //     given:
    //     def config = [
    //             kafka : [
    //                     url:kafkaURL(),
    //                     group:'group'
    //             ]
    //     ]

    //     writeKafkaMessage("test", "Hola")
    //     writeKafkaMessage("test", "Hi")
    //     writeKafkaMessage("test", "end")
    //     when:
    //     def SCRIPT = '''
    //         include { watchTopic } from 'plugin/nf-kafka'
    //         process listener{
    //             input: val(msg)
    //             output: stdout 
    //             script: 
    //             "echo ${msg.value}"
    //         }            
    //         chn = channel.watchTopic("test", [key:null, value:"end"])
    //         workflow{
    //             main:
    //                 listener(chn)
    //                 sleep 3000
    //             emit:
    //                 listener.out
    //         }
            
    //         '''
    //     and:
    //     def result = new MockScriptRunner(config).setScript(SCRIPT).execute()

    //     then:
    //     [result.val, result.val, result.val].sort() == ['echo Hola', 'echo Hi', 'echo end'].sort()
    // }

    // def 'cand write into and read from a topic' () {
    //     given:
    //     def config = [
    //             kafka : [
    //                     url:kafkaURL(),
    //                     group:'group'
    //             ]
    //     ]

    //     when:
    //     def SCRIPT = '''
    //         include { writeMessage; watchTopic } from 'plugin/nf-kafka'
    //         process listener{
    //             input: val(msg)
    //             output: stdout 
    //             script:            
    //             "echo ${msg.value}"
    //         }            
            
    //         chn = channel.watchTopic("test", [key:null, value:"end"])

    //         writeMessage('test', 'Hi folks')
    //         writeMessage('test', 'end', null)

    //         workflow{
    //             main:
    //                 listener(chn)
    //             emit:
    //                 listener.out
    //         }
            
    //         '''
    //     and:
    //     def result = new MockScriptRunner(config).setScript(SCRIPT).execute()

    //     then:
    //     result.val == 'echo end'
    // }

}

