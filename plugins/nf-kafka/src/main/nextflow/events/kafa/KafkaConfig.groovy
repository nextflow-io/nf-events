package nextflow.events.kafa

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * @author : jorge <jorge.aguilera@seqera.io>
 *
 */
@ToString(includePackage = false, includeNames = true)
@EqualsAndHashCode(includeFields = true)
@CompileStatic
class KafkaConfig {

    private String url // mandatory
    private String group // mandatory
    private Integer pollTimeout // ms

    KafkaConfig(Map map){
        def config = map ?: Collections.emptyMap()
        
        url = config.url ?: KafkaConfig.missingConf("url")
        group = config.group ?: KafkaConfig.missingConf("group")
        pollTimeout = KafkaConfig.parsePollTimeout(config.pollTimeout)
    }

    String getUrl(){
        url
    }

    String getGroup(){
        group
    }
    
    Integer getpollTimeout(){
        pollTimeout
    }

    static private Integer parsePollTimeout(confPollTimeout) {
        Integer pollTimeout = 1000 // default value
        if (confPollTimeout) {
            pollTimeout = confPollTimeout
                            .toString()
                            .toInteger()
        } 
        return pollTimeout
    }

    static private missingConf(String conf) {
        throw new KafkaConfigException(conf);
    }

}

class KafkaConfigException extends Exception {
    public KafkaConfigException(String conf) {
        super(
            "the configuration of the $conf is mandatory, please configure it in the scope of the kafka plugin conf"
        );
    }
}