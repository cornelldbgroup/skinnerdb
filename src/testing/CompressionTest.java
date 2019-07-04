package testing;

import buffer.BufferManager;
import catalog.CatalogManager;
import data.IntData;
import query.ColumnRef;

public class CompressionTest {

	public static void main(String[] args) throws Exception {
		CatalogManager.loadDB("/Users/immanueltrummer/"
				+ "Documents/Temp/SkinnerSchema/imdb.db");
		ColumnRef colRef = new ColumnRef("title", "title");
		IntData intData = (IntData)BufferManager.getData(colRef);
		for (int i=0; i<10; ++i) {
			System.out.println(intData.data[i]);
		}
	}

}
