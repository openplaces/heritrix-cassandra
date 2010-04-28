package org.archive.modules.writer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;

public class CassandraWriterProcessor extends WriterPoolProcessor {

	private static final long serialVersionUID = 6207244931489760644L;

	@Override
	long getDefaultMaxFileSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	List<String> getDefaultStorePaths() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<String> getMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected ProcessResult innerProcessResult(CrawlURI uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void setupPool(AtomicInteger serial) {
		// TODO Auto-generated method stub
		
	}

}
