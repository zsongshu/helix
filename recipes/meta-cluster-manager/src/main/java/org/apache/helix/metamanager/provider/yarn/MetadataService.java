package org.apache.helix.metamanager.provider.yarn;

import java.util.Collection;

public interface MetadataService {

	public boolean exists(String id);

	public void create(ContainerMetadata meta) throws MetadataServiceException;

	public ContainerMetadata read(String id) throws MetadataServiceException;

	public Collection<ContainerMetadata> readAll() throws MetadataServiceException;

	public void update(ContainerMetadata meta) throws MetadataServiceException;

	public void delete(String id) throws MetadataServiceException;

	public static class MetadataServiceException extends Exception {

		/**
		 * 
		 */
		private static final long serialVersionUID = -2846997013918977056L;

		public MetadataServiceException() {
			super();
		}

		public MetadataServiceException(String message, Throwable cause) {
			super(message, cause);
		}

		public MetadataServiceException(String message) {
			super(message);
		}

		public MetadataServiceException(Throwable cause) {
			super(cause);
		}	
	}
}