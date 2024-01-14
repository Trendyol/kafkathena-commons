package com.trendyol.mpc.kafkathena.commons.util.extensions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trendyol.mpc.kafkathena.commons.model.DemoModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
public class KSJsonSupportTest implements KSJsonSupport {
    @Spy
    KSJsonSupport ksJsonSupport;

    public void a(DemoModel m) {
        System.out.println(asJson(new ObjectMapper(), m));
    }

    @Test
    public void it_should_AsJson() {
        //given
        DemoModel demoModel = new DemoModel();
        demoModel.setAge(10);
        demoModel.setName("Demo");
        String result = "{\"name\":\"Demo\",\"age\":10}";
        //when

        String s = ksJsonSupport.asJson(new ObjectMapper(), demoModel);
        //then
        assertThat(s).isEqualTo(result);
    }

}