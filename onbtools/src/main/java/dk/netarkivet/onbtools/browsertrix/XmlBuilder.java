package dk.netarkivet.onbtools.browsertrix;

import java.io.StringWriter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;

public abstract class XmlBuilder {
    protected final Document xmlDoc;
    private static DocumentBuilder builder;

    public XmlBuilder() {
        xmlDoc = getParser().newDocument();
    }

    public XmlBuilder(Document xmlDoc) {
        this.xmlDoc = xmlDoc;
    }

    public org.dom4j.Document getDoc() {
        org.dom4j.io.DOMReader reader = new org.dom4j.io.DOMReader();
        return reader.read(xmlDoc);
    }

    protected static synchronized DocumentBuilder getParser() {
        if (builder == null) {
            try {
                DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
                documentBuilderFactory.setNamespaceAware(true);
                builder = documentBuilderFactory.newDocumentBuilder();
            } catch (ParserConfigurationException e) {
                throw new RuntimeException(e);
            }
        }
        return builder;
    }

    /**
     * Creates a default XmlDoc based on the order.xml file on the classpath.
     * @return The loaded default XmlDoc.
     */
    protected static synchronized Document parseFile(String name) {
        try {
            return getParser().parse(XmlBuilder.class.getClassLoader().getResourceAsStream(name));
        } catch (Exception e) {
            throw new RuntimeException("Failed to read " +  name + " from path " +
                    XmlBuilder.class.getClassLoader().getResource(name), e);
        }
    }

    @Override
    public String toString() {
        try {
            DOMSource domSource = new DOMSource(xmlDoc);
            StringWriter writer = new StringWriter();
            StreamResult result = new StreamResult(writer);
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = null;
            transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.transform(domSource, result);
            writer.flush();
            return writer.toString();
        } catch (TransformerException e) {
            throw new RuntimeException(e);
        }
    }
}
