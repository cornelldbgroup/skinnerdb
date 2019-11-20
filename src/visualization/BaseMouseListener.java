package visualization;

import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

/**
 * No-op mouse listener class that others can extend to override
 * specific methods.
 */
public class BaseMouseListener implements MouseListener {
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
}
