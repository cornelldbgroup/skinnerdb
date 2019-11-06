package visualization;

import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.ui.spriteManager.Sprite;
import org.graphstream.ui.spriteManager.SpriteManager;
import org.graphstream.ui.swingViewer.ViewPanel;
import org.graphstream.ui.view.View;
import org.graphstream.ui.view.Viewer;
import org.graphstream.ui.view.ViewerListener;
import org.graphstream.ui.view.ViewerPipe;

import java.awt.event.MouseListener;

public class Visualization implements ViewerListener {
    public static void main(String[] args) {
        (new Visualization()).run();
    }

    private boolean loop = true;

    private void run() {
        System.setProperty("org.graphstream.ui.renderer",
                "org.graphstream.ui.j2dviewer.J2DGraphRenderer");

        SingleGraph graph = new SingleGraph("g1");
        Viewer viewer = graph.display();

        SpriteManager sman = new SpriteManager(graph);

        View view = viewer.getDefaultView();
        for (MouseListener listener : ((ViewPanel) view).getMouseListeners()) {
            view.removeMouseListener(listener);
        }


        ViewerPipe pipeIn = viewer.newViewerPipe();
        pipeIn.addSink(graph);
        pipeIn.addViewerListener(this);

        graph.setAttribute("ui.stylesheet", "sprite { shape: flow; size: 5px;" +
                " z-index: 0; } sprite#S1 { fill-color: #373; } sprite#S2 { " +
                "fill-color: #393; } sprite#S3 { fill-color: #3B3; }");
        graph.setAttribute("ui.antialias");
        graph.setAttribute("ui.quality");

        graph.addNode("A");
        graph.addNode("B");
        graph.addNode("C");
        graph.addEdge("AB", "A", "B");
        graph.addEdge("BC", "B", "C");
        graph.addEdge("CA", "C", "A");
        graph.getNode("A").setAttribute("xyz", new double[]{-1, 0, 0});
        graph.getNode("B").setAttribute("xyz", new double[]{1, 0, 0});
        graph.getNode("C").setAttribute("xyz", new double[]{0, 1, 0});

        SpriteManager sm = new SpriteManager(graph);
        Sprite s1 = sm.addSprite("S1");
        Sprite s2 = sm.addSprite("S2");
        Sprite s3 = sm.addSprite("S3");

        s1.attachToEdge("AB");
        s2.attachToEdge("BC");
        s3.attachToEdge("CA");
        s1.setPosition(0);
        s2.setPosition(0);
        s3.setPosition(0);

        double s1pos = 0, s2pos = 0, s3pos = 0;

        int i = 1;

        while (loop) {
            pipeIn.pump();
            sleep(500);

            if (s1pos < 1.0) {
                s1pos += 0.1;
                s2pos += 0.1;
                s3pos += 0.1;
                s1.setPosition(s1pos);
                s2.setPosition(s2pos);
                s3.setPosition(s3pos);
            }
        }

        System.out.println("bye bye");
        System.exit(0);
    }

    protected void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) { e.printStackTrace(); }
    }

// Viewer Listener Interface

    public void viewClosed(String id) { loop = false;}

    public void buttonPushed(String id) {
        System.out.println(id);
        if (id.equals("quit"))
            loop = false;
        else if (id.equals("A"))
            System.out.println("Button A pushed");
    }

    public void buttonReleased(String id) {}

    // Data
    private String styleSheet = ""
            + "graph {"
            + "	canvas-color: white; "
            + "	fill-mode: gradient-radial; "
            + "	fill-color: white, #EEEEEE; "
            + "	padding: 60px; "
            + "}"
            + ""
            + "node {"
            + "	shape: freeplane;"
            + "	size: 10px;"
            + "	size-mode: fit;"
            + "	fill-mode: none;"
            + "	stroke-mode: plain;"
            + "	stroke-color: grey;"
            + "	stroke-width: 3px;"
            + "	padding: 5px, 1px;"
            + "	shadow-mode: none;"
            + "	icon-mode: at-left;"
            + "	text-style: normal;"
            + "	text-font: 'Droid Sans';"
            + "}"
            + ""
            + "node:clicked {"
            + "	stroke-mode: plain;"
            + "	stroke-color: red;"
            + "}"
            + ""
            + "node:selected {"
            + "	stroke-mode: plain;"
            + "	stroke-color: blue;"
            + "}"
            + ""
            + "edge {"
            + "	shape: freeplane;"
            + "	size: 3px;"
            + "	fill-color: grey;"
            + "	fill-mode: plain;"
            + "	shadow-mode: none;"
            + "	shadow-color: rgba(0,0,0,100);"
            + "	shadow-offset: 3px, -3px;"
            + "	shadow-width: 0px;"
            + "	arrow-shape: arrow;"
            + "	arrow-size: 20px, 6px;"
            + "}"
            + ""
            + "sprite {"
            + " shape: flow;"
            + " fill-color: green;"
            + "}";

    public void mouseOver(String id) {}

    public void mouseLeft(String id) {}
}