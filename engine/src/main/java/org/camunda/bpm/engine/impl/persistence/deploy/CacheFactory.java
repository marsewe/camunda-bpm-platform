/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.camunda.bpm.engine.impl.persistence.deploy;

import org.camunda.commons.utils.cache.Cache;

/**
 * <p>Builds the caches for the {@link DeploymentCache}.</p>
 */
public interface CacheFactory<String, DbEntity> {

  /**
   * Creates a cache that does not exceed a specified number of elements.
   *
   * @param maxNumberOfElementsInCache
   *        The maximum number of elements that is allowed within the cache at the same time.
   * @return
   *        The cache to be created.
   */
  public Cache<String, DbEntity> createCache(int maxNumberOfElementsInCache);
}