
import com.cmit.cmhk.entity.LabelRule;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
//import org.codehaus.jackson.JsonParser;
//import org.codehaus.jackson.map.ObjectMapper;


public class jsonTest {

    public static void main(String[] args) throws Exception {
        String str = "{'singleRule':false,'logic':'and','ruleList':[{'singleRule':true,'rule':'>','attribute':'1','value':'6'},{'singleRule':true,'rule':'>=','attribute':'2','value':'6'},{'singleRule':false,'logic':'and','ruleList':[{'singleRule':true,'rule':'>','attribute':'3','value':'6'},{'singleRule':true,'rule':'>','attribute':'4','value':'6'}]}]}";
        jsonTest.transRule(str);
//        String str = "(true || true || false && false)";
//        System.out.println(str);
//        ScriptEngineManager manager = new ScriptEngineManager();
//        ScriptEngine engine = manager.getEngineByName("js");
//        Object result = engine.eval(str);

//        System.out.println("结果类型:" + result.getClass().getName() + ",计算结果:" + result);

    }

    public static void transRule(String jsonRule) {

        LabelRule labelRule = null;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES,true);
            labelRule = objectMapper.readValue(jsonRule,LabelRule.class);
            String flag = parse(labelRule);
            System.out.println(flag);
        } catch (Exception e) {
            System.out.println("Json文件解析错误：" + e);
        }

    }

    public static String parse(LabelRule labelRule) {
        String str = "";
        if(labelRule.isSingleRule()) {//单个条件
//            System.out.println("logic:"+labelRule.getLogic());
//            System.out.println("ruleList:"+labelRule.getRuleList());
//            System.out.println("rule:"+labelRule.getRule());
//            System.out.println("attribute:"+labelRule.getAttribute());
//            System.out.println("value:"+labelRule.getValue());
            str = str + true;
        } else {
            if(null != labelRule.getRuleList() && 0 < labelRule.getRuleList().size()) {
//                System.out.println("logic:"+labelRule.getLogic());
                str = str + "(";
                for(int i=0;i < labelRule.getRuleList().size();i++) {
                   str = str + parse(labelRule.getRuleList().get(i));
                   if(i == labelRule.getRuleList().size() - 1) {
                       str = str + ")";
                   } else {
                       str = str + " " + "&&" + " ";
                   }
                }
            };
        }
        return str;
    }
}
