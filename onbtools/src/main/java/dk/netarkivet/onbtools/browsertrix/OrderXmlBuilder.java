package dk.netarkivet.onbtools.browsertrix;

public class OrderXmlBuilder extends XmlBuilder {

	public static final String DEFAULT_ORDER_XML_NAME = "FullSite-order";
    public static final String ORDER_XML_NAME = "h3.xml";

    private OrderXmlBuilder() {}
    private OrderXmlBuilder(String name) { super(parseFile(name)); }

    public static OrderXmlBuilder create() {
        return new OrderXmlBuilder();
    }
    public static OrderXmlBuilder createDefault() { return createDefault(ORDER_XML_NAME); }
    public static OrderXmlBuilder createDefault(String name) { return new OrderXmlBuilder(name);}
}
