package org.apache.helix.metamanager.impl.yarn;

import java.util.Collection;

interface MetadataProvider {

	public boolean exists(String id);

	public void create(ContainerMetadata meta) throws MetadataException;

	public ContainerMetadata read(String id) throws MetadataException;

	public Collection<ContainerMetadata> readAll() throws MetadataException;

	public void update(ContainerMetadata meta) throws MetadataException;

	public void delete(String id) throws MetadataException;
	
	public static class MetadataException extends Exception {

		/**
		 * 
		 */
		private static final long serialVersionUID = -2846997013918977056L;

		public MetadataException() {
			super();
		}

		public MetadataException(String message, Throwable cause) {
			super(message, cause);
		}

		public MetadataException(String message) {
			super(message);
		}

		public MetadataException(Throwable cause) {
			super(cause);
		}	
	}
}