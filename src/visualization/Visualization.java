package visualization;

import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.ui.spriteManager.Sprite;
import org.graphstream.ui.spriteManager.SpriteManager;
import org.graphstream.ui.swingViewer.ViewPanel;
import org.graphstream.ui.view.View;
import org.graphstream.ui.view.Viewer;
import org.graphstream.ui.view.ViewerListener;
import org.graphstream.ui.view.ViewerPipe;

import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

public class Visualization implements ViewerListener, MouseListener {
    private boolean loop = true;

    public void update(int[] joinOrder, double reward, int[] tupleIndices,
                       int[] tableCardinality) {

    }

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
        view.addMouseListener(this);


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
        graph.getNode("A").setAttribute("xyz", new double[]{-1, 0, 0});
        graph.getNode("B").setAttribute("xyz", new double[]{1, 0, 0});
        graph.getNode("C").setAttribute("xyz", new double[]{0, 1, 0});

        SpriteManager sm = new SpriteManager(graph);
        Sprite s1 = sm.addSprite("S1");
        Sprite s2 = sm.addSprite("S2");

        s1.attachToEdge("AB");
        s2.attachToEdge("BC");
        s1.setPosition(0);
        s2.setPosition(0);

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

    public void viewClosed(String id) { loop = false;}

    public void buttonPushed(String id) {
        System.out.println(id);
        if (id.equals("quit"))
            loop = false;
        else if (id.equals("A"))
            System.out.println("Button A pushed");
    }

    public void buttonReleased(String id) {}

    @Override
    public void mouseClicked(MouseEvent e) {}

    @Override
    public void mousePressed(MouseEvent e) {}

    @Override
    public void mouseReleased(MouseEvent e) {}

    @Override
    public void mouseEntered(MouseEvent e) {}

    @Override
    public void mouseExited(MouseEvent e) {}

    private static Visualization INSTANCE = new Visualization();

    public static Visualization get() {
        return INSTANCE;
    }
}