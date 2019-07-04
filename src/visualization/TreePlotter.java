package visualization;

import java.io.FileOutputStream;

import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Document;
import com.itextpdf.text.pdf.PdfContentByte;
import com.itextpdf.text.pdf.PdfWriter;

import joining.uct.UctNode;

/**
 * Provides functions to visualize the UCT search tree
 * used during join order learning.
 * 
 * @author immanueltrummer
 *
 */
public class TreePlotter {
	/**
	 * Draw sub-tree via recursive invocations.
	 * 
	 * @param root			root of sub-tree to plot
	 * @param xOffset		root node is placed at this x coordinate
	 * @param xRange		width of horizontal range usable for subtree
	 * @param yStep			tree levels are that far apart on y axis
	 * @param yOffset		place root of sub-tree at this y coordinate
	 * @param pdfContent	reference to pdf canvas
	 */
	static void plotSubtree(UctNode root, int xRange, 
			int xOffset, int yStep, int yOffset,
			PdfContentByte pdfContent) {
		// Check for null pointer
		if (root != null) {
			// Calculate center of node to draw
			int yCenter = yOffset;
			int xCenter = xOffset;
			// Draw this node
			pdfContent.setColorStroke(BaseColor.RED);
			pdfContent.setColorFill(BaseColor.RED);
			pdfContent.circle(xCenter, yCenter, 2);
			pdfContent.fillStroke();
		    // Treat child nodes
		    int nrActions = root.nrActions;
		    if (nrActions>0) {
		    	// Draw child nodes and connections
		    	int childXrange = Math.floorDiv(xRange, nrActions);		    	
				for (int childCtr=0; childCtr<nrActions; ++childCtr) {
					// Draw connection to child node
					int childXoffset = xOffset - xRange/2 + 
							childXrange * childCtr + childXrange/2;
					int childYoffset = yCenter - 25;
					pdfContent.setLineWidth(1);
					pdfContent.setColorStroke(BaseColor.BLACK);
					pdfContent.moveTo(xCenter, yCenter);
					pdfContent.lineTo(childXoffset, childYoffset);
					pdfContent.stroke();
					// Draw child tree
					UctNode child = root.childNodes[childCtr];
					plotSubtree(child, childXrange, childXoffset, 
							yStep, childYoffset, pdfContent);
				}	    	
		    }			
		}
	}
	/**
	 * Draws sub-tree starting at given root into a pdf
	 * document at the given path.
	 * 
	 * @param root	root of sub-tree to draw
	 * @param path	create drawing at this path
	 * @throws Exception
	 */
	public static void plotTree(UctNode root, String path) throws Exception {
	    Document.compress = false;
	    Document document = new Document();
	    PdfWriter writer = PdfWriter.getInstance(
	    		document, new FileOutputStream(path));
	    document.open();
	    PdfContentByte cb = writer.getDirectContent();
	    int xRange = (int)document.getPageSize().getRight();
	    int xCenter = Math.floorDiv(xRange, 2);
	    int yRange = (int)document.getPageSize().getTop();
	    int yStep = Math.floorDiv(yRange, root.nrActions);
	    plotSubtree(root, xRange, xCenter, yStep, yRange, cb);
	    document.close();
	}
}
